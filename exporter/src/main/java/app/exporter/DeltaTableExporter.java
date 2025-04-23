package app.exporter;

import app.common.model.DeltaLogEntry;
import app.common.model.ExportMetadata;
import app.common.storage.StorageProvider;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
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
    private static final int BUFFER_SIZE = 8192;

    private final String tablePath;
    private final long fromVersion;
    private final String outputZipPath;
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
     * @param outputZipPath The local path where the ZIP file will be created
     * @param tempDir       The temporary directory to use for downloading files
     * @param storageProvider The storage provider to use for file operations
     */
    public DeltaTableExporter(String tablePath, long fromVersion,
                             String outputZipPath, String tempDir,
                             StorageProvider storageProvider) {
        this.tablePath = tablePath;
        this.fromVersion = fromVersion;
        this.outputZipPath = outputZipPath;
        this.tempDir = tempDir;
        this.gson = new GsonBuilder().create();
        this.processedFiles = new HashSet<>();
        this.storageProvider = storageProvider;
        this.actualEndVersion = -1;
    }

    /**
     * Exports the Delta table to a ZIP file.
     *
     * @throws IOException If an I/O error occurs
     * @return The path to the created ZIP file, which may include version information
     */
    public String export() throws IOException {
        LOG.info("Starting export of Delta table from {} to local ZIP {}", tablePath, outputZipPath);
        
        // Find the latest available version before starting the export
        long latestVersion = findLatestVersion();
        
        LOG.info("Exporting versions {} to {} (latest)", fromVersion, latestVersion);

        // Create temporary directory if it doesn't exist
        Files.createDirectories(Paths.get(tempDir));
        
        // Download and process Delta log files (using latest version as upper bound)
        downloadDeltaLogFiles(latestVersion);
        
        // Download last checkpoint file if it exists
        downloadLastCheckpointFile();
        
        // Generate metadata file
        generateMetadataFile();
        
        // Create the final ZIP file
        String finalOutputPath = createZipArchive();
        
        // Close the storage provider
        storageProvider.close();
        
        LOG.info("Export completed successfully to {}", finalOutputPath);
        return finalOutputPath;
    }

    /**
     * Finds the latest available version in the Delta table.
     *
     * @return The latest version number, or 0 if no versions are found
     * @throws IOException If an I/O error occurs
     */
    private long findLatestVersion() throws IOException {
        String deltaLogPath = DELTA_LOG_DIR;
        
        if (!storageProvider.directoryExists(deltaLogPath)) {
            throw new IOException("Delta log directory not found: " + deltaLogPath);
        }
        
        // Find the highest version number by scanning the log directory
        long highestVersion = -1;
        try {
            List<String> logFiles = storageProvider.listFiles(deltaLogPath, "\\d{20}\\.json");
            
            highestVersion = logFiles.stream()
                .map(path -> {
                    String fileName = Paths.get(path).getFileName().toString();
                    return Long.parseLong(fileName.substring(0, 20));
                })
                .max(Long::compare)
                .orElse(0L);
        } catch (Exception e) {
            LOG.warn("Error finding latest version: {}", e.getMessage());
            return 0;
        }
        
        if (highestVersion < 0) {
            LOG.warn("No version files found in Delta log directory");
            return 0;
        }
        
        LOG.info("Found latest Delta table version: {}", highestVersion);
        return highestVersion;
    }

    /**
     * Downloads Delta log files for the specified version range.
     *
     * @param actualToVersion The actual ending version to use (latest available)
     * @throws IOException If an I/O error occurs
     */
    private void downloadDeltaLogFiles(long actualToVersion) throws IOException {
        String deltaLogPath = DELTA_LOG_DIR;
        
        if (!storageProvider.directoryExists(deltaLogPath)) {
            throw new IOException("Delta log directory not found: " + deltaLogPath);
        }

        // Create local delta log directory
        Files.createDirectories(Paths.get(tempDir, DELTA_LOG_DIR));
        
        // Process each version in the specified range
        for (long version = fromVersion; version <= actualToVersion; version++) {
            String logFileName = String.format("%020d.json", version);
            String logCheckpointFileName = String.format("%020d.checkpoint.parquet", version);

            String logFilePath = deltaLogPath + "/" + logFileName;
            String logCheckpointFilePath = deltaLogPath + "/" + logCheckpointFileName;

            if (storageProvider.fileExists(logFilePath)) {
                LOG.info("Processing Delta log file: {}", logFileName);
                
                // Download the log file
                String localLogPath = Paths.get(tempDir, DELTA_LOG_DIR, logFileName).toString();
                storageProvider.downloadFile(logFilePath, localLogPath);

                // Download the log checkpoint file, if present
                if (storageProvider.fileExists(logCheckpointFilePath)) {
                    LOG.info("Processing Delta log checkpoint file: {}", logCheckpointFileName);
                    String localCheckpointLogPath = Paths.get(tempDir, DELTA_LOG_DIR, logCheckpointFileName).toString();
                    storageProvider.downloadFile(logCheckpointFilePath, localCheckpointLogPath);
                }
                
                // Process the log file to extract data file paths
                processLogFile(localLogPath);
                
                // Update the actual end version
                actualEndVersion = version;
            } else {
                LOG.warn("Delta log file not found for version {}: {}", version, logFilePath);
                return;
            }
        }
    }

    /**
     * Downloads the _last_checkpoint file if it exists in the source table.
     * This file is important for Delta Lake's state management.
     *
     * @throws IOException If an I/O error occurs
     */
    private void downloadLastCheckpointFile() throws IOException {
        String checkpointPath = DELTA_LOG_DIR + "/" + LAST_CHECKPOINT_FILE;
        
        // Path for the local _last_checkpoint file
        String localCheckpointPath = Paths.get(tempDir, DELTA_LOG_DIR, LAST_CHECKPOINT_FILE).toString();
        
        if (storageProvider.fileExists(checkpointPath)) {
            LOG.info("Found _last_checkpoint file, downloading it");
            storageProvider.downloadFile(checkpointPath, localCheckpointPath);
        } else {
            LOG.info("_last_checkpoint file not found in source table, will not be included in export");
        }
    }

    /**
     * Processes a Delta log file to extract and download referenced data files.
     *
     * @param logFilePath The local path to the log file
     * @throws IOException If an I/O error occurs
     */
    private void processLogFile(String logFilePath) throws IOException {
        // First, read the entire log file and parse all entries
        List<DeltaLogEntry> logEntries = parseLogFile(logFilePath);
        LOG.info("Parsed {} log entries from {}", logEntries.size(), logFilePath);
        
        // Then process each entry to extract and download the required files
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
        File logFile = new File(logFilePath);
        
        try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
            String line;
            int lineNumber = 0;
            
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                try {
                    DeltaLogEntry logEntry = gson.fromJson(line, DeltaLogEntry.class);
                    entries.add(logEntry);
                } catch (JsonParseException e) {
                    LOG.error("Error parsing Delta log entry in file '{}' at line {}: {}", 
                             logFile.getName(), lineNumber, e.getMessage());
                    LOG.debug("Problematic JSON content: {}", line);
                    throw new IOException(String.format(
                            "Failed to parse Delta log entry in file '%s' at line %d: %s", 
                            logFile.getName(), lineNumber, e.getMessage()), e);
                }
            }
        } catch (IOException e) {
            LOG.error("Error reading Delta log file '{}': {}", logFile.getName(), e.getMessage());
            throw new IOException(String.format(
                    "Error reading Delta log file '%s': %s", 
                    logFile.getName(), e.getMessage()), e);
        }
        
        LOG.info("Successfully parsed {} log entries from file '{}'", entries.size(), logFile.getName());
        return entries;
    }
    
    /**
     * Processes a single Delta log entry to extract and download referenced files.
     *
     * @param logEntry The Delta log entry to process
     * @throws IOException If an I/O error occurs
     */
    private void processLogEntry(DeltaLogEntry logEntry) throws IOException {
        // Process add operations to extract data file paths
        if (logEntry.getAdd() != null && logEntry.getAdd().getPath() != null) {
            String dataFilePath = logEntry.getAdd().getPath();
            downloadDataFile(dataFilePath);
            
            // Process deletion vector files if present (dvFile)
            if (logEntry.getAdd().getDeletionVector() != null) {
                DeltaLogEntry.DeletionVector dv = logEntry.getAdd().getDeletionVector();
                processStandardDeletionVectorFormats(dv);
            }
            // Process deletionVectorDescriptor (modern Delta)
            if (logEntry.getAdd().getDeletionVectorDescriptor() != null) {
                DeltaLogEntry.DeletionVectorDescriptor dvDesc = logEntry.getAdd().getDeletionVectorDescriptor();
                if (dvDesc.getStoragePath() != null) {
                    String dvPath = dvDesc.getStoragePath();
                    LOG.info("Found deletion vector file (descriptor.storagePath): {}", dvPath);
                    downloadDeletionVectorFile(dvPath);
                }
            }
        }
        
        // Also check for deletion vectors in remove operations (Delta 3.x+)
        if (logEntry.getRemove() != null) {
            if (logEntry.getRemove().getDeletionVector() != null) {
                DeltaLogEntry.DeletionVector dv = logEntry.getRemove().getDeletionVector();
                processStandardDeletionVectorFormats(dv);
                
                // Handle pathOrInlineDv field which may contain an inline DV or a reference to a DV file
                if (dv.getPathOrInlineDv() != null) {
                    String pathOrValue = dv.getPathOrInlineDv();
                    String storageType = dv.getStorageType();
                    
                    if ("p".equals(storageType)) {
                        // It's a direct path reference
                        LOG.info("Found deletion vector path reference: {}", pathOrValue);
                        downloadDeletionVectorFile(pathOrValue); 
                    } else {
                        // For other storage types (e.g., "u" for ULEB128), we need to search for actual DV files
                        // Scan for deletion vector files in the root directory
                        LOG.info("Scanning for deletion vector files in source table root for inline DV reference");
                        scanForDeletionVectorFiles();
                    }
                }
            }
        }
    }

    /**
     * Process the standard deletion vector formats: dvFile and storagePath
     */
    private void processStandardDeletionVectorFormats(DeltaLogEntry.DeletionVector dv) throws IOException {
        if (dv.getDvFile() != null) {
            String dvFilePath = dv.getDvFile();
            LOG.info("Found deletion vector file: {}", dvFilePath);
            downloadDeletionVectorFile(dvFilePath);
            // Check for stats file
            String dvFileBasePath = dvFilePath.substring(0, dvFilePath.lastIndexOf('.'));
            String dvStatsFilePath = dvFileBasePath + ".stats";
            if (storageProvider.fileExists(dvStatsFilePath)) {
                LOG.info("Found deletion vector stats file: {}", dvStatsFilePath);
                downloadDataFile(dvStatsFilePath);
            }
        }
        // Process storagePath (modern Delta)
        if (dv.getStoragePath() != null) {
            String dvPath = dv.getStoragePath();
            LOG.info("Found deletion vector file (storagePath): {}", dvPath);
            downloadDeletionVectorFile(dvPath);
        }
    }
    
    /**
     * Scans for deletion vector files in the source table root directory
     */
    private void scanForDeletionVectorFiles() throws IOException {
        if (storageProvider.directoryExists("")) {
            List<String> dvFiles = storageProvider.listFiles("", "deletion_vector_.*\\.bin");
            for (String dvPath : dvFiles) {
                LOG.info("Found deletion vector file by scanning: {}", dvPath);
                downloadDeletionVectorFile(dvPath);
            }
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
        // Skip if we've already processed this file
        if (processedFiles.contains(dvFilePath)) {
            LOG.debug("Deletion vector file already processed: {}", dvFilePath);
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
            LOG.info("Successfully downloaded deletion vector file: {}", dvFilePath);
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
        // Skip if we've already processed this file
        if (processedFiles.contains(dataFilePath)) {
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
     * Creates the final ZIP archive from the downloaded files.
     *
     * @throws IOException If an I/O error occurs
     * @return The path to the created ZIP file
     */
    private String createZipArchive() throws IOException {
        // Use the exact output path provided without modifying it
        String finalOutputPath = outputZipPath;
        
        // Ensure it has a .zip extension if not already present
        if (!finalOutputPath.toLowerCase().endsWith(".zip")) {
            finalOutputPath += ".zip";
        }
        
        LOG.info("Creating ZIP archive: {}", finalOutputPath);
        
        Path tempPath = Paths.get(tempDir);
        try (FileOutputStream fos = new FileOutputStream(finalOutputPath);
             ZipOutputStream zipOut = new ZipOutputStream(fos)) {
            
            Files.walk(tempPath)
                .filter(path -> !Files.isDirectory(path))
                .forEach(path -> {
                    try {
                        // Create ZIP entry with relative path
                        String relativePath = tempPath.relativize(path).toString();
                        ZipEntry zipEntry = new ZipEntry(relativePath);
                        zipOut.putNextEntry(zipEntry);
                        
                        // Write file contents to ZIP
                        Files.copy(path, zipOut);
                        zipOut.closeEntry();
                    } catch (IOException e) {
                        LOG.error("Error adding file to ZIP: {}", path, e);
                    }
                });
        }
        
        return finalOutputPath;
    }
}
