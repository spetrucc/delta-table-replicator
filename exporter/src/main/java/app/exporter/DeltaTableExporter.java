package app.exporter;

import app.common.model.DeltaLogEntry;
import app.common.model.ExportMetadata;
import app.common.storage.StorageProvider;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZOutputFile;
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

/**
 * Exports Delta Lake tables from S3 or local filesystem to a local 7-Zip archive.
 * The 7-Zip archive contains all necessary files to replicate the table between two Delta versions.
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
    private long maxVolumeBytes;

    /**
     * Creates a new DeltaTableExporter.
     *
     * @param tablePath     The path to the Delta table (s3a://bucket/path/to/table or file:///path/to/table)
     * @param fromVersion   The starting version to export (inclusive)
     * @param outputArchivePath The local path where the 7-Zip archive will be created
     * @param tempDir       The temporary directory to use for downloading files
     * @param storageProvider The storage provider to use for file operations
     */
    public DeltaTableExporter(String tablePath, long fromVersion,
                             String outputArchivePath, String tempDir,
                             StorageProvider storageProvider) {
        this(tablePath, fromVersion, outputArchivePath, tempDir, storageProvider, 2L * 1024 * 1024 * 1024); // Default 2GB
    }

    /**
     * Creates a new DeltaTableExporter with a specified maximum volume size.
     *
     * @param tablePath     The path to the Delta table (s3a://bucket/path/to/table or file:///path/to/table)
     * @param fromVersion   The starting version to export (inclusive)
     * @param outputArchivePath The local path where the 7-Zip archive will be created
     * @param tempDir       The temporary directory to use for downloading files
     * @param storageProvider The storage provider to use for file operations
     * @param maxVolumeBytes The maximum size for each 7-Zip volume in bytes
     */
    public DeltaTableExporter(String tablePath, long fromVersion,
                             String outputArchivePath, String tempDir,
                             StorageProvider storageProvider, long maxVolumeBytes) {
        this.tablePath = tablePath;
        this.fromVersion = fromVersion;
        this.outputArchivePath = outputArchivePath;
        this.tempDir = tempDir;
        this.gson = new GsonBuilder().create();
        this.processedFiles = new HashSet<>();
        this.storageProvider = storageProvider;
        this.actualEndVersion = -1;
        this.maxVolumeBytes = maxVolumeBytes > 0 ? maxVolumeBytes : 2L * 1024 * 1024 * 1024; // Ensure positive value
    }

    /**
     * Sets the maximum size for each 7-Zip volume in bytes.
     * If a 7-Zip archive would exceed this size, it will be split into multiple volumes.
     * 
     * @param maxVolumeBytes Maximum size in bytes for each 7-Zip volume
     */
    public void setMaxVolumeBytes(long maxVolumeBytes) {
        this.maxVolumeBytes = maxVolumeBytes > 0 ? maxVolumeBytes : 2L * 1024 * 1024 * 1024;
    }

    /**
     * Gets the maximum size for each 7-Zip volume in bytes.
     * 
     * @return Maximum size in bytes for each 7-Zip volume
     */
    public long getMaxVolumeBytes() {
        return maxVolumeBytes;
    }

    /**
     * Exports the Delta table to a 7-Zip archive.
     * 
     * @throws IOException If an I/O error occurs
     * @return The path to the created 7-Zip archive, which may include version information
     */
    public String export() throws IOException {
        // Find the latest available version in the Delta table
        long latestVersion = findLatestVersion();
        if (latestVersion < 0) {
            throw new IOException("No versions found in the Delta table");
        }
        
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
        
        // Create the 7-Zip archive from the downloaded files
        return create7ZipArchive();
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
     * Creates the final 7-Zip archive from the downloaded files.
     * If the files exceed the maximum volume size, multiple volumes will be created.
     *
     * @throws IOException If an I/O error occurs
     * @return The path to the created 7-Zip archive (or path pattern for multi-volume archives)
     */
    private String create7ZipArchive() throws IOException {
        // Ensure the output path has a .7z extension
        String baseOutputPath = outputArchivePath;
        if (!baseOutputPath.toLowerCase().endsWith(".7z")) {
            baseOutputPath += ".7z";
        }
        
        LOG.info("Creating 7-Zip archive: {}", baseOutputPath);
        
        // Collect all files to be archived
        Path tempPath = Paths.get(tempDir);
        List<Path> allFiles = Files.walk(tempPath)
            .filter(path -> !Files.isDirectory(path))
            .toList();
        
        if (allFiles.isEmpty()) {
            LOG.warn("No files found to add to 7-Zip archive");
            // Create an empty archive
            try (SevenZOutputFile sevenZOutput = new SevenZOutputFile(new File(baseOutputPath))) {
                // Empty archive created
            }
            return baseOutputPath;
        }
        
        // Estimate if we need multiple volumes
        long totalSize = 0;
        for (Path path : allFiles) {
            totalSize += Files.size(path);
        }
        
        LOG.info("Total size of files to archive: {} bytes", totalSize);
        
        // If the total size is small enough for a single file
        if (totalSize < maxVolumeBytes) {
            LOG.info("Creating single 7-Zip archive");
            createSingle7ZipArchive(baseOutputPath, tempPath, allFiles);
            return baseOutputPath;
        } else {
            LOG.info("Creating multi-volume 7-Zip archive with max volume size: {} bytes", maxVolumeBytes);
            return createMultiVolume7ZipArchive(baseOutputPath, tempPath, allFiles);
        }
    }
    
    /**
     * Creates a single 7-Zip archive containing all files.
     *
     * @param outputPath The output path for the 7-Zip archive
     * @param basePath The base path for calculating relative paths
     * @param files The list of files to include
     * @throws IOException If an I/O error occurs
     */
    private void createSingle7ZipArchive(String outputPath, Path basePath, List<Path> files) throws IOException {
        try (SevenZOutputFile sevenZOutput = new SevenZOutputFile(new File(outputPath))) {
            for (Path path : files) {
                String relativePath = basePath.relativize(path).toString();
                SevenZArchiveEntry entry = new SevenZArchiveEntry();
                entry.setName(relativePath);
                entry.setSize(Files.size(path));
                sevenZOutput.putArchiveEntry(entry);
                
                // Use InputStream for copying
                try (InputStream inputStream = Files.newInputStream(path)) {
                    byte[] buffer = new byte[8192];
                    int bytesRead;
                    while ((bytesRead = inputStream.read(buffer)) != -1) {
                        sevenZOutput.write(buffer, 0, bytesRead);
                    }
                }
                
                sevenZOutput.closeArchiveEntry();
            }
        }
    }
    
    /**
     * Creates a multi-volume 7-Zip archive with files distributed across volumes.
     *
     * @param baseOutputPath The base output path for the 7-Zip volumes
     * @param basePath The base path for calculating relative paths
     * @param files The list of files to include
     * @throws IOException If an I/O error occurs
     * @return Pattern for the created volumes
     */
    private String createMultiVolume7ZipArchive(String baseOutputPath, Path basePath, List<Path> files) throws IOException {
        // Split the files into volumes
        int volumeNumber = 1;
        int totalVolumes = 0;
        long currentVolumeSize = 0;
        List<Path> currentVolumeFiles = new ArrayList<>();
        
        // Get the base name without extension for volume naming
        String baseNameWithoutExt = baseOutputPath;
        if (baseNameWithoutExt.toLowerCase().endsWith(".7z")) {
            baseNameWithoutExt = baseNameWithoutExt.substring(0, baseNameWithoutExt.length() - 3);
        }
        
        // Process all files, distributing them across volumes
        for (Path path : files) {
            long fileSize = Files.size(path);
            
            // If this file would make the current volume too big, create the current volume
            if (currentVolumeSize + fileSize > maxVolumeBytes && !currentVolumeFiles.isEmpty()) {
                // Create a volume with the current batch of files
                String volumePath = String.format("%s.vol%03d.7z", baseNameWithoutExt, volumeNumber);
                createSingle7ZipArchive(volumePath, basePath, currentVolumeFiles);
                
                LOG.info("Created volume {} with {} files, {} bytes", 
                        volumeNumber, currentVolumeFiles.size(), currentVolumeSize);
                
                // Reset for next volume
                volumeNumber++;
                currentVolumeFiles = new ArrayList<>();
                currentVolumeSize = 0;
                totalVolumes++;
            }
            
            // Add the current file to the batch
            currentVolumeFiles.add(path);
            currentVolumeSize += fileSize;
        }
        
        // Create final volume with remaining files
        if (!currentVolumeFiles.isEmpty()) {
            String volumePath = String.format("%s.vol%03d.7z", baseNameWithoutExt, volumeNumber);
            createSingle7ZipArchive(volumePath, basePath, currentVolumeFiles);
            
            LOG.info("Created final volume {} with {} files, {} bytes", 
                    volumeNumber, currentVolumeFiles.size(), currentVolumeSize);
            totalVolumes++;
        }
        
        LOG.info("Created {} volumes for the 7-Zip archive", totalVolumes);
        
        // Return pattern for multi-volume files
        if (totalVolumes == 1) {
            return String.format("%s.vol001.7z", baseNameWithoutExt);
        } else {
            return baseNameWithoutExt + ".vol*.7z";
        }
    }
}
