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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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

        // TODO: Handle case where fromVersion > latestVersion
        // TODO: Ensure that we have all commit since 0 OR at least 1 checkpoint

        // Use the latest available version as the end version
        actualEndVersion = latestVersion;
        LOG.info("Exporting Delta table from version {} to {} (latest)", fromVersion, actualEndVersion);
        
        // Create temporary directory if it doesn't exist
        Files.createDirectories(Paths.get(tempDir));
        
        // Download the Delta log files for the specified version range
        downloadDeltaLogFiles(actualEndVersion);
        
        // Download the _last_checkpoint file if it exists
        downloadLastCheckpointFile();
        
        // Scan for deletion vector files
        scanForDeletionVectorFiles();
        
        // Generate metadata.json file
        generateMetadataFile();
        
        // Create the ZIP archive from the downloaded files
        return createZipArchive();
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
     * @throws IOException If an I/O error occurs
     */
    private void downloadDeltaLogFiles(long actualToVersion) throws IOException {
        LOG.info("Downloading Delta log files from version {} to {}", fromVersion, actualToVersion);
        
        String logDir = tablePath + "/" + DELTA_LOG_DIR;
        String localLogDir = tempDir + "/" + DELTA_LOG_DIR;
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
                processLogFile(localJsonLogPath);
            } else {
                LOG.warn("Delta log file not found: {}", jsonLogFile);
            }
            
            // Check for checkpoint file
            String checkpointFile = String.format("%020d.checkpoint.parquet", v);
            String checkpointPath = logDir + "/" + checkpointFile;
            String localCheckpointPath = localLogDir + "/" + checkpointFile;
            
            if (storageProvider.fileExists(checkpointPath)) {
                LOG.info("Downloading Delta checkpoint file: {}", checkpointFile);
                storageProvider.downloadFile(checkpointPath, localCheckpointPath);
            }
        }
        
        LOG.info("Delta log files downloaded successfully");
    }

    /**
     * Downloads the _last_checkpoint file if it exists in the source table.
     * This file is important for Delta Lake's state management.
     * 
     * @throws IOException If an I/O error occurs
     */
    private void downloadLastCheckpointFile() throws IOException {
        LOG.info("Checking for _last_checkpoint file");
        
        String lastCheckpointPath = tablePath + "/" + DELTA_LOG_DIR + "/" + LAST_CHECKPOINT_FILE;
        String localLastCheckpointPath = tempDir + "/" + DELTA_LOG_DIR + "/" + LAST_CHECKPOINT_FILE;
        
        if (storageProvider.fileExists(lastCheckpointPath)) {
            LOG.info("Downloading _last_checkpoint file");
            storageProvider.downloadFile(lastCheckpointPath, localLastCheckpointPath);
        } else {
            LOG.info("_last_checkpoint file not found");
        }
    }

    /**
     * Processes a Delta log file to extract and download referenced data files.
     * 
     * @param logFilePath The local path to the log file
     * @throws IOException If an I/O error occurs
     */
    private void processLogFile(String logFilePath) throws IOException {
        LOG.info("Processing log file: {}", logFilePath);
        
        // Parse the log file
        List<DeltaLogEntry> logEntries = parseLogFile(logFilePath);
        LOG.info("Found {} log entries", logEntries.size());
        
        // Process each log entry
        for (DeltaLogEntry logEntry : logEntries) {
            processLogEntry(logEntry);
        }
    }

    /**
     * Parses a Delta log file into a list of DeltaLogEntry objects.
     * 
     * @param logFilePath The local path to the log file
     * @return A list of parsed DeltaLogEntry objects
     * @throws IOException If an I/O error occurs
     */
    private List<DeltaLogEntry> parseLogFile(String logFilePath) throws IOException {
        List<DeltaLogEntry> entries = new ArrayList<>();
        
        try (BufferedReader reader = new BufferedReader(new FileReader(logFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    DeltaLogEntry entry = gson.fromJson(line, DeltaLogEntry.class);
                    entries.add(entry);
                }
            }
        }
        
        return entries;
    }

    /**
     * Processes a single Delta log entry to extract and download referenced files.
     * 
     * @param logEntry The Delta log entry to process
     * @throws IOException If an I/O error occurs
     */
    private void processLogEntry(DeltaLogEntry logEntry) throws IOException {
        // Process "add" actions to download data files
        if (logEntry.getAdd() != null) {
            String dataFilePath = logEntry.getAdd().getPath();
            downloadDataFile(dataFilePath);
            
            // Check for deletion vectors in add operations
            if (logEntry.getAdd().getDeletionVector() != null) {
                processStandardDeletionVectorFormats(logEntry.getAdd().getDeletionVector());
            }
            
            // Check for deletion vector descriptor in add operations
            if (logEntry.getAdd().getDeletionVectorDescriptor() != null) {
                if (logEntry.getAdd().getDeletionVectorDescriptor().getStoragePath() != null) {
                    downloadDeletionVectorFile(logEntry.getAdd().getDeletionVectorDescriptor().getStoragePath());
                }
            }
        }
        
        // Check for deletion vectors in remove operations
        if (logEntry.getRemove() != null && logEntry.getRemove().getDeletionVector() != null) {
            processStandardDeletionVectorFormats(logEntry.getRemove().getDeletionVector());
        }
    }

    /**
     * Process the standard deletion vector formats: dvFile and storagePath
     * 
     * @param dv The deletion vector object from the log entry
     * @throws IOException If an I/O error occurs
     */
    private void processStandardDeletionVectorFormats(DeltaLogEntry.DeletionVector dv) throws IOException {
        // Check for files referenced by the deletion vector
        if (dv.getDvFile() != null && !dv.getDvFile().isEmpty()) {
            String dvFilePath = dv.getDvFile();
            downloadDeletionVectorFile(dvFilePath);
        }
        
        if (dv.getStoragePath() != null && !dv.getStoragePath().isEmpty()) {
            String dvStoragePath = dv.getStoragePath();
            downloadDeletionVectorFile(dvStoragePath);
        }
    }

    /**
     * Scans for deletion vector files in the source table root directory
     * 
     * @throws IOException If an I/O error occurs
     */
    private void scanForDeletionVectorFiles() throws IOException {
        LOG.info("Scanning for deletion vector files in Delta table root");
        
        // Check for deletion vector directory
        String dvDirectoryPath = tablePath + "/deletion_vector";
        
        if (storageProvider.fileExists(dvDirectoryPath)) {
            LOG.info("Found deletion_vector directory, downloading files");
            
            // List files in the deletion_vector directory (use ".*" to match all files)
            List<String> dvFiles = storageProvider.listFiles(dvDirectoryPath, ".*");
            
            for (String dvFile : dvFiles) {
                // Get the relative path to the deletion vector file
                String relativePath = "deletion_vector/" + dvFile;
                downloadDeletionVectorFile(relativePath);
            }
        } else {
            LOG.info("No deletion_vector directory found");
        }
    }

    /**
     * Downloads a deletion vector file from the source filesystem to the local temporary directory.
     * This is similar to downloadDataFile but with specific handling for deletion vector files.
     * 
     * @param dvFilePath The relative path to the deletion vector file
     * @throws IOException If an I/O error occurs
     */
    private void downloadDeletionVectorFile(String dvFilePath) throws IOException {

        // TODO: Extend the use of processedFiles to also cover data files; data files might be repeated across versions
        // Skip if already processed
        if (processedFiles.contains(dvFilePath)) {
            LOG.info("Deletion vector file already processed: {}", dvFilePath);
            return;
        }
        
        if (storageProvider.fileExists(dvFilePath)) {
            LOG.info("Downloading deletion vector file: {}", dvFilePath);
            
            // Create parent directories if needed
            String localDvFilePath = Paths.get(tempDir, dvFilePath).toString();
            Files.createDirectories(Paths.get(localDvFilePath).getParent());
            
            // Download the file
            storageProvider.downloadFile(dvFilePath, localDvFilePath);
            
            // Mark as processed
            processedFiles.add(dvFilePath);
        } else {
            LOG.warn("Deletion vector file not found: {}", dvFilePath);
        }
    }

    /**
     * Downloads a data file from the source filesystem to the local temporary directory.
     * 
     * @param dataFilePath The relative path to the data file
     * @throws IOException If an I/O error occurs
     */
    private void downloadDataFile(String dataFilePath) throws IOException {
        // Skip if already processed
        if (processedFiles.contains(dataFilePath)) {
            LOG.info("Data file already processed: {}", dataFilePath);
            return;
        }
        
        if (storageProvider.fileExists(dataFilePath)) {
            LOG.info("Downloading data file: {}", dataFilePath);
            
            // Create parent directories if needed
            String localDataFilePath = Paths.get(tempDir, dataFilePath).toString();
            Files.createDirectories(Paths.get(localDataFilePath).getParent());
            
            // Download the file
            storageProvider.downloadFile(dataFilePath, localDataFilePath);
            
            // Mark as processed
            processedFiles.add(dataFilePath);
        } else {
            LOG.warn("Data file not found: {}", dataFilePath);
        }
    }

    /**
     * Generates a metadata.json file with information about the export.
     *
     * @throws IOException If an I/O error occurs
     */
    private void generateMetadataFile() throws IOException {
        LOG.info("Generating export metadata file");
        
        // Create metadata object
        ExportMetadata metadata = new ExportMetadata(tablePath, fromVersion, actualEndVersion);
        
        // Write metadata to file
        String metadataPath = Paths.get(tempDir, "metadata.json").toString();
        try (FileWriter writer = new FileWriter(metadataPath)) {
            gson.toJson(metadata, writer);
        }
        
        LOG.info("Metadata file generated successfully");
    }

    /**
     * Creates the final ZIP archive from the downloaded files using Java's native ZIP implementation.
     *
     * @throws IOException If an I/O error occurs
     * @return The path to the created ZIP archive
     */
    private String createZipArchive() throws IOException {
        // Ensure the output path has a .zip extension
        String outputPath = outputArchivePath;
        if (!outputPath.toLowerCase().endsWith(".zip")) {
            outputPath += ".zip";
        }
        
        LOG.info("Creating ZIP archive: {}", outputPath);

        // TODO: Handle temp directory differently; create TEMP sub-folder and clean content after ZIP creation
        // Check for empty temp directory
        Path tempPath = Paths.get(tempDir);
        if (!Files.exists(tempPath) || Files.list(tempPath).findAny().isEmpty()) {
            LOG.warn("No files found to add to ZIP archive");
            return outputPath; // Return the output path even though no archive will be created
        }
        
        // Debug: Log all files to be archived
        LOG.info("Files to be archived:");
        Files.walk(tempPath).filter(Files::isRegularFile).forEach(file -> {
            try {
                LOG.info("  - {}: {} bytes", tempPath.relativize(file), Files.size(file));
            } catch (IOException e) {
                LOG.warn("Error getting file size: {}", file);
            }
        });
        
        try (FileOutputStream fos = new FileOutputStream(outputPath);
             ZipOutputStream zos = new ZipOutputStream(fos)) {
            
            Files.walk(tempPath)
                .filter(Files::isRegularFile)
                .forEach(file -> {
                    try {
                        String relativePath = tempPath.relativize(file).toString();
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
