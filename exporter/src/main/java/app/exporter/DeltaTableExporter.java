package app.exporter;

import app.common.model.DeltaLogEntry;
import app.common.model.ExportMetadata;
import app.common.storage.StorageProvider;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Exports Delta Lake tables from S3 or local filesystem to a local ZIP archive.
 * The ZIP archive contains all necessary files to replicate the table between two Delta versions.
 */
public class DeltaTableExporter {
    private static final Logger LOG = LoggerFactory.getLogger(DeltaTableExporter.class);
    private static final String DELTA_LOG_DIR = "_delta_log";
    private static final String LAST_CHECKPOINT_FILE = "_last_checkpoint";

    private final String tablePath;
    private final long fromVersion;
    private final String outputArchivePath;
    private final String tempDir;
    private final Gson gson;
    private final Set<String> processedFiles;
    private final StorageProvider storageProvider;
    private long actualEndVersion;

    /**
     * Creates a new DeltaTableExporter.
     *
     * @param tablePath     The path to the Delta table (s3a://bucket/path/to/table or file:///path/to/table)
     * @param fromVersion   The starting version to export (inclusive)
     * @param outputArchivePath The local path where the ZIP archive will be created
     * @param tempDir       The temporary directory to use for downloading files
     * @param storageProvider The storage provider to use for file operations
     */
    public DeltaTableExporter(String tablePath, long fromVersion,
                             String outputArchivePath, String tempDir,
                             StorageProvider storageProvider) {
        this.tablePath = tablePath;
        this.fromVersion = fromVersion;
        this.outputArchivePath = outputArchivePath;
        this.tempDir = tempDir;
        this.gson = new GsonBuilder().create();
        this.processedFiles = new HashSet<>();
        this.storageProvider = storageProvider;
        this.actualEndVersion = -1;
    }

    /**
     * Exports the Delta table to a ZIP archive.
     * 
     * @throws IOException If an I/O error occurs
     * @return The path to the created ZIP archive
     */
    public String export() throws IOException {
        // Find the latest available version in the Delta table
        long latestVersion = findLatestVersion();
        if (latestVersion < 0) {
            throw new IOException("No versions found in the Delta table");
        }

        // Handle case where fromVersion > latestVersion
        if (fromVersion > latestVersion) {
            throw new IOException("Requested start version " + fromVersion + 
                " is greater than the latest available version " + latestVersion);
        }

        // Ensure we have all commits since 0 OR at least 1 checkpoint
        boolean hasRequiredHistory = ensureRequiredHistory();
        if (!hasRequiredHistory) {
            throw new IOException("Cannot export table: required history not available. " +
                "Either all commits since version 0 or at least one checkpoint file must be present.");
        }

        // Use the latest available version as the end version
        actualEndVersion = latestVersion;
        LOG.info("Exporting Delta table from version {} to {} (latest)", fromVersion, actualEndVersion);
        
        // Create temporary directory if it doesn't exist
        Path tempPath = Paths.get(tempDir);
        Files.createDirectories(tempPath);
        
        // Create a subfolder for the actual content to be zipped
        Path contentTempPath = tempPath.resolve("content");
        Files.createDirectories(contentTempPath);
        String contentTempDir = contentTempPath.toString();
        
        // Download the Delta log files for the specified version range
        downloadDeltaLogFiles(actualEndVersion, contentTempDir);
        
        // Download the _last_checkpoint file if it exists
        downloadLastCheckpointFile(contentTempDir);
        
        // Scan for deletion vector files
        scanForDeletionVectorFiles(contentTempDir);
        
        // Generate metadata.json file
        generateMetadataFile(contentTempDir);
        
        // Create the ZIP archive from the downloaded files
        String zipPath = createZipArchive(contentTempPath);
        
        // Clean up the content temp directory
        LOG.info("Cleaning up temporary content directory: {}", contentTempPath);
        try {
            Files.walk(contentTempPath)
                .sorted((a, b) -> -a.compareTo(b))
                .forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        LOG.warn("Failed to delete: {}", path, e);
                    }
                });
        } catch (IOException e) {
            LOG.warn("Error cleaning up temporary content directory: {}", e.getMessage());
        }
        
        return zipPath;
    }

    /**
     * Ensures that the required history is available for the export.
     * Either all commits since version 0 or at least one checkpoint file must be present.
     *
     * @return true if required history is available, false otherwise
     * @throws IOException If an I/O error occurs
     */
    private boolean ensureRequiredHistory() throws IOException {
        String logDir = tablePath + "/" + DELTA_LOG_DIR;
        
        // Check if we're starting from version 0
        if (fromVersion == 0) {
            return true; // Starting from 0 means we have all history
        }
        
        // Check if at least one checkpoint file exists for any version up to the fromVersion
        for (long v = fromVersion; v >= 0; v--) {
            String checkpointFile = String.format("%s/%020d.checkpoint.parquet", logDir, v);
            if (storageProvider.fileExists(checkpointFile)) {
                LOG.info("Found checkpoint file for version {}", v);
                return true;
            }
        }
        
        // Check if all commits exist from 0 to fromVersion
        boolean hasAllCommits = true;
        for (long v = 0; v < fromVersion; v++) {
            String jsonLogFile = String.format("%s/%020d.json", logDir, v);
            if (!storageProvider.fileExists(jsonLogFile)) {
                LOG.warn("Missing commit log file for version {}", v);
                hasAllCommits = false;
                break;
            }
        }
        
        return hasAllCommits;
    }

    /**
     * Finds the latest available version in the Delta table.
     * 
     * @return The latest version number, or 0 if no versions are found
     * @throws IOException If an I/O error occurs
     */
    private long findLatestVersion() throws IOException {
        LOG.info("Finding the latest available version in the Delta table");
        
        // Check for JSON files in _delta_log directory starting from the specified fromVersion
        String logDir = tablePath + "/" + DELTA_LOG_DIR;
        long latestVersion = -1;
        
        for (long v = fromVersion; ; v++) {
            String jsonLogFile = String.format("%s/%020d.json", logDir, v);
            if (!storageProvider.fileExists(jsonLogFile)) {
                // No more versions
                if (v > fromVersion) {
                    // If we've found at least one version, return the last one found
                    latestVersion = v - 1;
                }
                break;
            }
            // If the file exists, continue checking the next version
            latestVersion = v;
        }
        
        if (latestVersion >= 0) {
            LOG.info("Latest available version: {}", latestVersion);
        } else {
            LOG.warn("No versions found starting from version {}", fromVersion);
        }
        
        return latestVersion;
    }

    /**
     * Downloads Delta log files for the specified version range.
     * 
     * @param actualToVersion The actual ending version to use (latest available)
     * @param contentTempDir The temporary directory to use for downloaded content
     * @throws IOException If an I/O error occurs
     */
    private void downloadDeltaLogFiles(long actualToVersion, String contentTempDir) throws IOException {
        LOG.info("Downloading Delta log files from version {} to {}", fromVersion, actualToVersion);
        
        String logDir = tablePath + "/" + DELTA_LOG_DIR;
        String localLogDir = contentTempDir + "/" + DELTA_LOG_DIR;
        Files.createDirectories(Paths.get(localLogDir));
        
        for (long v = fromVersion; v <= actualToVersion; v++) {
            // Download the JSON log file
            String jsonLogFile = String.format("%020d.json", v);
            String jsonLogPath = logDir + "/" + jsonLogFile;
            String localJsonLogPath = localLogDir + "/" + jsonLogFile;
            
            if (storageProvider.fileExists(jsonLogPath)) {
                LOG.info("Downloading Delta log file: {}", jsonLogFile);
                storageProvider.downloadFile(jsonLogPath, localJsonLogPath);
                
                // Process the log file to download data files
                processLogFile(localJsonLogPath, contentTempDir);
            } else {
                LOG.warn("Delta log file not found: {}", jsonLogFile);
            }
            
            // Check for checkpoint file
            String checkpointFile = String.format("%020d.checkpoint.parquet", v);
            String checkpointPath = logDir + "/" + checkpointFile;
            String localCheckpointPath = localLogDir + "/" + checkpointFile;
            
            if (storageProvider.fileExists(checkpointPath)) {
                LOG.info("Downloading checkpoint file: {}", checkpointFile);
                storageProvider.downloadFile(checkpointPath, localCheckpointPath);
            }
        }
    }

    /**
     * Downloads the _last_checkpoint file if it exists.
     *
     * @param contentTempDir The temporary directory to use for downloaded content 
     * @throws IOException If an I/O error occurs
     */
    private void downloadLastCheckpointFile(String contentTempDir) throws IOException {
        LOG.info("Checking for _last_checkpoint file");
        
        String checkpointPath = tablePath + "/" + DELTA_LOG_DIR + "/" + LAST_CHECKPOINT_FILE;
        String localCheckpointPath = contentTempDir + "/" + DELTA_LOG_DIR + "/" + LAST_CHECKPOINT_FILE;
        
        if (storageProvider.fileExists(checkpointPath)) {
            LOG.info("Downloading _last_checkpoint file");
            storageProvider.downloadFile(checkpointPath, localCheckpointPath);
        } else {
            LOG.info("_last_checkpoint file not found");
        }
    }

    /**
     * Scans the Delta log files for deletion vector files and downloads them.
     *
     * @param contentTempDir The temporary directory to use for downloaded content
     * @throws IOException If an I/O error occurs
     */
    private void scanForDeletionVectorFiles(String contentTempDir) throws IOException {
        LOG.info("Scanning for deletion vector files");
        
        String localLogDir = contentTempDir + "/" + DELTA_LOG_DIR;
        File logDir = new File(localLogDir);
        
        // Check if the directory exists
        if (!logDir.exists() || !logDir.isDirectory()) {
            LOG.warn("Delta log directory not found: {}", localLogDir);
            return;
        }
        
        // Get all JSON log files
        File[] logFiles = logDir.listFiles((dir, name) -> name.endsWith(".json"));
        if (logFiles != null) {
            for (File logFile : logFiles) {
                try (FileReader reader = new FileReader(logFile);
                     BufferedReader bufferedReader = new BufferedReader(reader)) {
                    String line;
                    while ((line = bufferedReader.readLine()) != null) {
                        if (line.contains("deletionVector")) {
                            // Parse the deletion vector file path
                            try {
                                DeltaLogEntry entry = gson.fromJson(line, DeltaLogEntry.class);
                                if (entry != null && entry.getAdd() != null && entry.getAdd().getDeletionVector() != null) {
                                    DeltaLogEntry.DeletionVector dv = entry.getAdd().getDeletionVector();
                                    // Try various path fields that might be present
                                    if (dv.getDvFile() != null && !dv.getDvFile().isEmpty()) {
                                        downloadDeletionVectorFile(dv.getDvFile(), contentTempDir);
                                    }
                                    if (dv.getStoragePath() != null && !dv.getStoragePath().isEmpty()) {
                                        downloadDeletionVectorFile(dv.getStoragePath(), contentTempDir);
                                    }
                                    if (dv.getPathOrInlineDv() != null && !dv.getPathOrInlineDv().isEmpty()) {
                                        downloadDeletionVectorFile(dv.getPathOrInlineDv(), contentTempDir);
                                    }
                                }
                                
                                // Also check remove operations
                                if (entry != null && entry.getRemove() != null && entry.getRemove().getDeletionVector() != null) {
                                    DeltaLogEntry.DeletionVector dv = entry.getRemove().getDeletionVector();
                                    // Try various path fields that might be present
                                    if (dv.getDvFile() != null && !dv.getDvFile().isEmpty()) {
                                        downloadDeletionVectorFile(dv.getDvFile(), contentTempDir);
                                    }
                                    if (dv.getStoragePath() != null && !dv.getStoragePath().isEmpty()) {
                                        downloadDeletionVectorFile(dv.getStoragePath(), contentTempDir);
                                    }
                                    if (dv.getPathOrInlineDv() != null && !dv.getPathOrInlineDv().isEmpty()) {
                                        downloadDeletionVectorFile(dv.getPathOrInlineDv(), contentTempDir);
                                    }
                                }
                                
                                // Check for deletion vector descriptors
                                if (entry != null && entry.getAdd() != null && 
                                    entry.getAdd().getDeletionVectorDescriptor() != null && 
                                    entry.getAdd().getDeletionVectorDescriptor().getStoragePath() != null) {
                                    downloadDeletionVectorFile(
                                        entry.getAdd().getDeletionVectorDescriptor().getStoragePath(), 
                                        contentTempDir
                                    );
                                }
                            } catch (Exception e) {
                                LOG.warn("Error parsing deletion vector from log entry: {}", e.getMessage());
                            }
                        }
                    }
                } catch (IOException e) {
                    LOG.warn("Error reading log file: {}", logFile.getName(), e);
                }
            }
        }
    }

    /**
     * Processes a Delta log file to find and download data files.
     *
     * @param logFilePath The path to the local log file
     * @param contentTempDir The temporary directory to use for downloaded content
     * @throws IOException If an I/O error occurs
     */
    private void processLogFile(String logFilePath, String contentTempDir) throws IOException {
        LOG.info("Processing log file: {}", logFilePath);
        
        try (FileReader reader = new FileReader(logFilePath);
             BufferedReader bufferedReader = new BufferedReader(reader)) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                // Look for "add" actions with "path" fields
                if (line.contains("\"add\"") && line.contains("\"path\"")) {
                    // Parse the data file path
                    try {
                        DeltaLogEntry entry = gson.fromJson(line, DeltaLogEntry.class);
                        if (entry != null && entry.getAdd() != null) {
                            String dataFilePath = entry.getAdd().getPath();
                            downloadDataFile(dataFilePath, contentTempDir);
                        }
                    } catch (Exception e) {
                        LOG.warn("Error parsing data file from log entry: {}", e.getMessage());
                    }
                }
            }
        }
    }

    /**
     * Downloads a data file from the Delta table.
     *
     * @param dataFilePath The path to the data file (relative to the table path)
     * @param contentTempDir The temporary directory to use for downloaded content
     * @throws IOException If an I/O error occurs
     */
    private void downloadDataFile(String dataFilePath, String contentTempDir) throws IOException {
        LOG.debug("Downloading data file: {}", dataFilePath);
        
        // Skip if already processed - using processedFiles to track all file types
        if (processedFiles.contains(dataFilePath)) {
            LOG.debug("Data file already processed: {}", dataFilePath);
            return;
        }
        
        // Get the full path to the data file
        String fullDataFilePath = tablePath + "/" + dataFilePath;
        String localDataFilePath = contentTempDir + "/" + dataFilePath;
        
        // Check if the file exists
        if (storageProvider.fileExists(fullDataFilePath)) {
            // Create parent directories if needed
            Files.createDirectories(Paths.get(localDataFilePath).getParent());
            
            // Download the file
            storageProvider.downloadFile(fullDataFilePath, localDataFilePath);
            
            // Mark as processed
            processedFiles.add(dataFilePath);
        } else {
            LOG.warn("Data file not found: {}", dataFilePath);
        }
    }

    /**
     * Downloads a deletion vector file from the Delta table.
     *
     * @param dvFilePath The path to the deletion vector file (relative to the table path)
     * @param contentTempDir The temporary directory to use for downloaded content
     * @throws IOException If an I/O error occurs
     */
    private void downloadDeletionVectorFile(String dvFilePath, String contentTempDir) throws IOException {
        // Skip if already processed - using processedFiles to track all file types
        if (processedFiles.contains(dvFilePath)) {
            LOG.info("Deletion vector file already processed: {}", dvFilePath);
            return;
        }
        
        LOG.info("Downloading deletion vector file: {}", dvFilePath);
        
        // Get the full path to the DV file
        String fullDvFilePath = tablePath + "/" + dvFilePath;
        String localDvFilePath = contentTempDir + "/" + dvFilePath;
        
        // Check if the file exists
        if (storageProvider.fileExists(fullDvFilePath)) {
            // Create parent directories if needed
            Files.createDirectories(Paths.get(localDvFilePath).getParent());
            
            // Download the file
            storageProvider.downloadFile(fullDvFilePath, localDvFilePath);
            
            // Mark as processed
            processedFiles.add(dvFilePath);
        } else {
            LOG.warn("Deletion vector file not found: {}", fullDvFilePath);
        }
    }

    /**
     * Generates a metadata.json file with information about the export.
     *
     * @param contentTempDir The temporary directory to use for downloaded content
     * @throws IOException If an I/O error occurs
     */
    private void generateMetadataFile(String contentTempDir) throws IOException {
        LOG.info("Generating export metadata file");
        
        // Create metadata object
        ExportMetadata metadata = new ExportMetadata(tablePath, fromVersion, actualEndVersion);
        
        // Write metadata to file
        String metadataPath = Paths.get(contentTempDir, "metadata.json").toString();
        try (FileWriter writer = new FileWriter(metadataPath)) {
            gson.toJson(metadata, writer);
        }
        
        LOG.info("Metadata file generated successfully");
    }

    /**
     * Creates the final ZIP archive from the downloaded files using Java's native ZIP implementation.
     *
     * @param contentTempPath The path to the temporary directory containing the content to archive
     * @throws IOException If an I/O error occurs
     * @return The path to the created ZIP archive
     */
    private String createZipArchive(Path contentTempPath) throws IOException {
        // Ensure the output path has a .zip extension
        String outputPath = outputArchivePath;
        if (!outputPath.toLowerCase().endsWith(".zip")) {
            outputPath += ".zip";
        }
        
        LOG.info("Creating ZIP archive: {}", outputPath);

        // Check for empty temp directory
        if (!Files.exists(contentTempPath) || Files.list(contentTempPath).findAny().isEmpty()) {
            LOG.warn("No files found to add to ZIP archive");
            return outputPath; // Return the output path even though no archive will be created
        }
        
        // Debug: Log all files to be archived
        LOG.info("Files to be archived:");
        Files.walk(contentTempPath).filter(Files::isRegularFile).forEach(file -> {
            try {
                LOG.info("  - {}: {} bytes", contentTempPath.relativize(file), Files.size(file));
            } catch (IOException e) {
                LOG.warn("Error getting file size: {}", file);
            }
        });
        
        try (FileOutputStream fos = new FileOutputStream(outputPath);
             ZipOutputStream zos = new ZipOutputStream(fos)) {
            
            Files.walk(contentTempPath)
                .filter(Files::isRegularFile)
                .forEach(file -> {
                    try {
                        String relativePath = contentTempPath.relativize(file).toString();
                        ZipEntry zipEntry = new ZipEntry(relativePath);
                        zos.putNextEntry(zipEntry);
                        
                        Files.copy(file, zos);
                        zos.closeEntry();
                        
                        LOG.debug("Added to archive: {}", relativePath);
                    } catch (IOException e) {
                        LOG.error("Error adding file to ZIP: {}", file, e);
                        throw new UncheckedIOException(e);
                    }
                });
            
            LOG.info("ZIP archive created successfully");
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
        
        // Debug: List the created archive
        File outputFile = new File(outputPath);
        if (outputFile.exists()) {
            LOG.info("ZIP archive created: {}: {} bytes", outputPath, outputFile.length());
        } else {
            LOG.error("ZIP archive creation failed: file does not exist");
            throw new IOException("ZIP archive creation failed: file does not exist");
        }
        
        return outputPath;
    }
}
