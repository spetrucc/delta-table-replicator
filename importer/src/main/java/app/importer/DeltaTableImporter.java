package app.importer;

import app.common.model.ExportMetadata;
import app.common.storage.StorageProvider;
import com.google.gson.Gson;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.DeltaLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Imports Delta Lake tables from a ZIP archive to a target location.
 * The ZIP archive should contain all necessary files to replicate the table.
 */
public class DeltaTableImporter {
    private static final Logger LOG = LoggerFactory.getLogger(DeltaTableImporter.class);
    private static final String DELTA_LOG_DIR = "_delta_log";

    private final String archivePath;
    private final String targetPath;
    private final String tempDir;
    private final boolean overwrite;
    private final boolean mergeSchema;
    private final StorageProvider storageProvider;
    private SparkSession spark;
    private ExportMetadata metadata;
    private final Gson gson;

    /**
     * Creates a new DeltaTableImporter.
     *
     * @param archivePath      The path to the ZIP archive containing the Delta table export
     * @param targetPath       The path where the Delta table will be created
     * @param tempDir          The temporary directory to use for extracting files
     * @param overwrite        Whether to overwrite the target table if it exists
     * @param mergeSchema      Whether to merge the schema with the existing table if it exists
     * @param storageProvider  The storage provider to use for file operations
     */
    public DeltaTableImporter(String archivePath, String targetPath, String tempDir, 
                             boolean overwrite, boolean mergeSchema,
                             StorageProvider storageProvider) {
        this.archivePath = archivePath;
        this.targetPath = targetPath;
        this.tempDir = tempDir;
        this.overwrite = overwrite;
        this.mergeSchema = mergeSchema;
        this.storageProvider = storageProvider;
        this.gson = new Gson();
    }

    /**
     * Imports the Delta table from the ZIP archive to the target location.
     *
     * @throws IOException If an I/O error occurs
     */
    public void importTable() throws IOException {
        LOG.info("Starting import of Delta table from archive {} to {}", archivePath, targetPath);

        // Create temporary directory if it doesn't exist
        Files.createDirectories(Paths.get(tempDir));
        
        // Extract the ZIP archive to the temporary directory
        extractZipArchive();

        // Read metadata - this is now mandatory
        if (!readMetadata()) {
            throw new IOException("Failed to import: metadata.json file is missing or invalid. " +
                    "The archive may be corrupted or not a valid Delta table export.");
        }

        // Initialize Spark
        initializeSpark();

        // Verify the extracted Delta table contents
        verifyExtractedContents();
        
        // Check that there are no more versions than expected
        if (!verifyVersionsMatch()) {
            throw new IOException("Failed to import: found version files that exceed the version range " +
                    "specified in metadata. The archive may be corrupted or incomplete.");
        }
        
        // Copy the extracted files to the target location
        copyToTarget();
        
        // Verify the imported table
        verifyImportedTable();
        
        // Close Spark session and storage provider
        if (spark != null) {
            spark.close();
        }
        
        storageProvider.close();
        
        LOG.info("Import completed successfully to {}", targetPath);
    }

    /**
     * Extracts the ZIP archive to the temporary directory using Java's ZIP implementation.
     *
     * @throws IOException If an I/O error occurs
     */
    private void extractZipArchive() throws IOException {
        LOG.info("Extracting ZIP archive: {}", archivePath);
        
        // Verify the archive file exists
        File archiveFile = new File(archivePath);
        if (!archiveFile.exists()) {
            throw new IOException("Archive file not found: " + archivePath);
        }
        
        // Create the temp directory if it doesn't exist
        File tempDirFile = new File(tempDir);
        if (!tempDirFile.exists()) {
            if (!tempDirFile.mkdirs()) {
                throw new IOException("Failed to create temp directory: " + tempDir);
            }
            LOG.info("Created temp directory: {}", tempDir);
        }
        
        try (ZipFile zipFile = new ZipFile(archiveFile)) {
            // Get the list of entries
            Enumeration<? extends ZipEntry> entries = zipFile.entries();
            
            // Extract each entry
            while (entries.hasMoreElements()) {
                ZipEntry entry = entries.nextElement();
                Path entryPath = Paths.get(tempDir, entry.getName());
                
                if (!entry.isDirectory()) {
                    // Ensure parent directories exist
                    Files.createDirectories(entryPath.getParent());
                    
                    // Extract the file using InputStream.transferTo
                    try (InputStream is = zipFile.getInputStream(entry);
                         FileOutputStream fos = new FileOutputStream(entryPath.toFile())) {
                        is.transferTo(fos);
                        LOG.debug("Extracted: {}", entry.getName());
                    }
                }
            }
            
            LOG.info("ZIP archive extracted successfully");
            
            // List files in the temp directory to verify extraction
            LOG.info("Listing files in temp directory after extraction:");
            listFiles(new File(tempDir), "");
            
            // Check if _delta_log directory exists in the extracted files
            File deltaLogDir = new File(tempDir, DELTA_LOG_DIR);
            if (!deltaLogDir.exists() || !deltaLogDir.isDirectory()) {
                LOG.error("_delta_log directory not found in extracted archive");
                // Try listing all directories to help diagnose
                LOG.info("Contents of temp directory:");
                for (File file : tempDirFile.listFiles()) {
                    LOG.info("  {}: {}", file.isDirectory() ? "Directory" : "File", file.getName());
                }
            } else {
                LOG.info("_delta_log directory found: {}", deltaLogDir.getAbsolutePath());
            }
        } catch (Exception e) {
            LOG.error("Error extracting ZIP archive", e);
            throw new IOException("ZIP archive extraction failed", e);
        }
    }
    
    /**
     * Recursively lists files in a directory for debugging purposes.
     *
     * @param dir Directory to list files from
     * @param indent Current indentation for prettier logging
     */
    private void listFiles(File dir, String indent) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    LOG.info("{}Directory: {}/", indent, file.getName());
                    listFiles(file, indent + "  ");
                } else {
                    LOG.info("{}File: {} ({} bytes)", indent, file.getName(), file.length());
                }
            }
        }
    }

    /**
     * Initializes the Spark session.
     */
    private void initializeSpark() {
        LOG.info("Initializing Spark session");
        
        spark = SparkSession.builder()
                .appName("DeltaTableImporter")
                .master("local[*]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate();
    }

    /**
     * Verifies that the extracted contents contain necessary files for a valid Delta table.
     *
     * @throws IOException If verification fails
     */
    private void verifyExtractedContents() throws IOException {
        LOG.info("Verifying extracted Delta table contents");
        
        File deltaLogDir = new File(tempDir, DELTA_LOG_DIR);
        if (!deltaLogDir.exists() || !deltaLogDir.isDirectory()) {
            throw new IOException("Invalid Delta table: _delta_log directory not found");
        }
        
        // Check if the directory contains any JSON log files or checkpoint files
        File[] logFiles = deltaLogDir.listFiles(file -> 
                file.getName().endsWith(".json") || file.getName().endsWith(".checkpoint.parquet"));
        
        if (logFiles == null || logFiles.length == 0) {
            throw new IOException("Invalid Delta table: no log files found in _delta_log directory");
        }
        
        LOG.info("Found {} log/checkpoint files in _delta_log directory", logFiles.length);
    }
    
    /**
     * Verifies that the Delta table versions match what's in the metadata.
     *
     * @return true if versions match, false otherwise
     */
    private boolean verifyVersionsMatch() {
        LOG.info("Verifying Delta table versions match metadata");
        
        if (metadata == null) {
            LOG.error("Cannot verify versions: metadata is missing");
            return false;
        }
        
        File deltaLogDir = new File(tempDir, DELTA_LOG_DIR);
        if (!deltaLogDir.exists() || !deltaLogDir.isDirectory()) {
            LOG.error("Cannot verify versions: _delta_log directory not found");
            return false;
        }
        
        // Get all version files (JSON or checkpoint)
        File[] versionFiles = deltaLogDir.listFiles(file -> {
            String name = file.getName();
            return (name.endsWith(".json") || name.endsWith(".checkpoint.parquet")) 
                   && name.length() >= 20; // Long enough to contain version number
        });
        
        if (versionFiles == null || versionFiles.length == 0) {
            LOG.warn("No version files found to verify");
            return true; // Can't verify, assume it's okay
        }
        
        long highestVersion = -1;
        
        // Find the highest version number in file names
        for (File file : versionFiles) {
            String name = file.getName();
            try {
                // Extract the version number (first 20 chars for our format)
                long version = Long.parseLong(name.substring(0, 20));
                highestVersion = Math.max(highestVersion, version);
            } catch (NumberFormatException e) {
                LOG.warn("Could not parse version from filename: {}", name);
            }
        }
        
        // Check if the highest version exceeds what's in metadata
        if (highestVersion > metadata.getToVersion()) {
            LOG.error("Found version {} which exceeds the maximum version {} in metadata", 
                    highestVersion, metadata.getToVersion());
            return false;
        }
        
        LOG.info("Verified highest version {} is within expected range (0 to {})", 
                highestVersion, metadata.getToVersion());
        return true;
    }

    /**
     * Reads the metadata.json file to extract information about the exported Delta table.
     *
     * @return true if metadata was successfully read, false otherwise
     */
    private boolean readMetadata() {
        LOG.info("Reading metadata.json file");
        
        File metadataFile = new File(tempDir, "metadata.json");
        if (!metadataFile.exists()) {
            LOG.error("metadata.json file not found in the extracted archive");
            return false;
        }
        
        try (FileReader reader = new FileReader(metadataFile)) {
            metadata = gson.fromJson(reader, ExportMetadata.class);
            
            if (metadata == null) {
                LOG.error("Failed to parse metadata.json file");
                return false;
            }
            
            LOG.info("Metadata read successfully:");
            LOG.info("  Source table path: {}", metadata.getTablePath());
            LOG.info("  From version: {}", metadata.getFromVersion());
            LOG.info("  To version: {}", metadata.getToVersion());
            
            return true;
        } catch (Exception e) {
            LOG.error("Error reading metadata.json file", e);
            return false;
        }
    }

    /**
     * Copies the Delta log files to the target location.
     *
     * @throws IOException If an I/O error occurs
     */
    private void copyDeltaLogFiles() throws IOException {
        LOG.info("Copying Delta log files to target");
        
        Path deltaLogSourcePath = Paths.get(tempDir, DELTA_LOG_DIR);
        if (!Files.exists(deltaLogSourcePath)) {
            LOG.error("Delta log directory not found in the extracted files");
            throw new IOException("Delta log directory not found in the extracted files");
        }
        
        // Create _delta_log directory in the target if it doesn't exist
        String targetDeltaLogPath = targetPath + "/" + DELTA_LOG_DIR;
        storageProvider.createDirectory(targetDeltaLogPath);
        
        // Copy each file in the _delta_log directory to the target location
        Files.walk(deltaLogSourcePath)
                .filter(Files::isRegularFile)
                .forEach(file -> {
                    try {
                        String relativePath = deltaLogSourcePath.relativize(file).toString();
                        String targetFilePath = targetPath + "/" + DELTA_LOG_DIR + "/" + relativePath;
                        
                        // Copy the file to the target using transferTo
                        try (InputStream is = Files.newInputStream(file);
                             OutputStream os = storageProvider.getOutputStream(targetFilePath)) {
                            is.transferTo(os);
                            LOG.debug("Copied Delta log file: {}", relativePath);
                        }
                    } catch (IOException e) {
                        LOG.error("Error copying Delta log file: {}", file, e);
                        throw new UncheckedIOException(e);
                    }
                });
        
        LOG.info("Delta log files copied successfully");
    }

    /**
     * Copies the data files to the target location.
     *
     * @throws IOException If an I/O error occurs
     */
    private void copyDataFiles() throws IOException {
        LOG.info("Copying data files to target");
        
        Path tempDirPath = Paths.get(tempDir);
        Path deltaLogPath = tempDirPath.resolve(DELTA_LOG_DIR);
        Path metadataPath = tempDirPath.resolve("metadata.json");
        
        // Copy all files except for files in _delta_log directory and metadata.json
        Files.walk(tempDirPath)
                .filter(Files::isRegularFile)
                .filter(file -> !file.startsWith(deltaLogPath) && !file.equals(metadataPath))
                .forEach(file -> {
                    try {
                        String relativePath = tempDirPath.relativize(file).toString();
                        String targetFilePath = targetPath + "/" + relativePath;

                        // Copy the file to the target using transferTo
                        try (InputStream is = Files.newInputStream(file);
                             OutputStream os = storageProvider.getOutputStream(targetFilePath)) {
                            is.transferTo(os);
                            LOG.debug("Copied data file: {}", relativePath);
                        }
                    } catch (IOException e) {
                        LOG.error("Error copying data file: {}", file, e);
                        throw new UncheckedIOException(e);
                    }
                });
        
        LOG.info("Data files copied successfully");
    }

    /**
     * Copies the extracted files to the target location.
     *
     * @throws IOException If an I/O error occurs
     */
    private void copyToTarget() throws IOException {
        LOG.info("Copying files to target location: {}", targetPath);
        
        // Check if target exists and handle according to options
        if (storageProvider.directoryExists(targetPath)) {
            if (overwrite) {
                LOG.info("Target exists, overwriting as requested");
                // If we're overwriting, we need to ensure the _delta_log directory exists
                storageProvider.createDirectory(targetPath + "/" + DELTA_LOG_DIR);
            } else if (!mergeSchema) {
                throw new IOException("Target path already exists. Use --overwrite to replace it or --merge-schema to merge with it.");
            }
        } else {
            // Create target directory if it doesn't exist
            storageProvider.createDirectory(targetPath);
            // Ensure _delta_log directory exists
            storageProvider.createDirectory(targetPath + "/" + DELTA_LOG_DIR);
        }
        
        // Copy Delta log files
        copyDeltaLogFiles();
        
        // Copy data files
        copyDataFiles();
        
        LOG.info("Files copied successfully to target location");
    }

    /**
     * Verifies that the imported table is valid and can be read.
     */
    private void verifyImportedTable() {
        try {
            LOG.info("Verifying imported table");
            
            // Attempt to load the Delta table
            DeltaLog deltaLog = DeltaLog.forTable(spark, targetPath);
            
            // Get the current version
            long version = deltaLog.snapshot().version();
            
            LOG.info("Successfully verified Delta table at {}, current version: {}", targetPath, version);
        } catch (Exception e) {
            LOG.warn("Warning: Could not verify imported table", e);
        }
    }
}
