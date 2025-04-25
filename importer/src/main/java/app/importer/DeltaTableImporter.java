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

        // TODO Make meta-data is mandatory; fail import if not present
        // Read metadata if available
        readMetadata();

        // TODO: Ensure that there was no more version than the one in the metadata

        // Initialize Spark
        initializeSpark();

        // TODO: This actually validates that the ZIP is not empty. Remove this step, or at least rename the method name to clarify
        // Validate the target Delta table
        validateDeltaTable();
        
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
                    
                    // Extract the file
                    // TODO: Update implementation using InputStream.transferTo
                    try (InputStream is = zipFile.getInputStream(entry);
                         FileOutputStream fos = new FileOutputStream(entryPath.toFile())) {
                        byte[] buffer = new byte[8192];
                        int length;
                        while ((length = is.read(buffer)) > 0) {
                            fos.write(buffer, 0, length);
                        }
                    }
                    LOG.debug("Extracted: {}", entry.getName());
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
     * Validates that the extracted files form a valid Delta table.
     *
     * @throws IOException If the validation fails
     */
    private void validateDeltaTable() throws IOException {
        LOG.info("Validating extracted Delta table");
        
        Path deltaLogPath = Paths.get(tempDir, DELTA_LOG_DIR);
        if (!Files.exists(deltaLogPath) || !Files.isDirectory(deltaLogPath)) {
            throw new IOException("Invalid Delta table: _delta_log directory not found");
        }
        
        // Check if there are any log files
        long logFileCount = Files.list(deltaLogPath)
                .filter(p -> p.toString().endsWith(".json"))
                .count();
        
        if (logFileCount == 0) {
            throw new IOException("Invalid Delta table: No log files found in _delta_log directory");
        }
        
        LOG.info("Delta table validation successful");
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
        
        // First, specifically ensure _delta_log directory and files are copied correctly
        Path deltaLogSourcePath = Paths.get(tempDir, DELTA_LOG_DIR);
        if (Files.exists(deltaLogSourcePath) && Files.isDirectory(deltaLogSourcePath)) {
            try (java.util.stream.Stream<Path> deltaLogFiles = Files.list(deltaLogSourcePath)) {
                deltaLogFiles.forEach(file -> {
                    try {
                        String relativePath = deltaLogSourcePath.relativize(file).toString();
                        String targetFilePath = targetPath + "/" + DELTA_LOG_DIR + "/" + relativePath;
                        
                        // TODO: Update implementation using InputStream.transferTo
                        // Copy the file to the target
                        try (InputStream is = Files.newInputStream(file);
                             OutputStream os = storageProvider.getOutputStream(targetFilePath)) {
                            byte[] buffer = new byte[8192];
                            int bytesRead;
                            while ((bytesRead = is.read(buffer)) != -1) {
                                os.write(buffer, 0, bytesRead);
                            }
                        }
                        
                        LOG.debug("Copied _delta_log file: {}", relativePath);
                    } catch (IOException e) {
                        LOG.error("Error copying _delta_log file: {}", file, e);
                    }
                });
            }
        }
        
        // Then copy data files
        Path tempDirPath = Paths.get(tempDir);
        try (java.util.stream.Stream<Path> dataFiles = Files.walk(tempDirPath)) {
            dataFiles
                .filter(Files::isRegularFile)
                .forEach(file -> {
                    try {
                        // Skip _delta_log files as we've already handled them
                        if (file.toString().contains(DELTA_LOG_DIR)) {
                            return;
                        }
                        
                        String relativePath = tempDirPath.relativize(file).toString();
                        String targetFilePath = targetPath + "/" + relativePath;

                        // TODO: Update implementation using InputStream.transferTo
                        // Copy the file to the target
                        try (InputStream is = Files.newInputStream(file);
                             OutputStream os = storageProvider.getOutputStream(targetFilePath)) {
                            byte[] buffer = new byte[8192];
                            int bytesRead;
                            while ((bytesRead = is.read(buffer)) != -1) {
                                os.write(buffer, 0, bytesRead);
                            }
                        }
                        
                        LOG.debug("Copied data file: {}", relativePath);
                    } catch (IOException e) {
                        LOG.error("Error copying data file: {}", file, e);
                    }
                });
        }
        
        LOG.info("Files copied successfully to target location");
    }

    /**
     * Reads the metadata file if it exists.
     */
    private void readMetadata() {
        try {
            Path metadataPath = Paths.get(tempDir, "export_metadata.json");
            if (Files.exists(metadataPath)) {
                String json = Files.readString(metadataPath);
                metadata = gson.fromJson(json, ExportMetadata.class);
                LOG.info("Read export metadata: table={}, fromVersion={}, toVersion={}, exportTime={}",
                        metadata.getTableName(), metadata.getFromVersion(),
                        metadata.getToVersion(), metadata.getExportTimestamp());
            } else {
                LOG.info("No metadata file found");
            }
        } catch (Exception e) {
            LOG.warn("Error reading metadata file", e);
        }
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
