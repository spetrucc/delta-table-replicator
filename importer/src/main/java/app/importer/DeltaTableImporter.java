package app.importer;

import app.common.storage.StorageProvider;
import app.common.model.ExportMetadata;
import com.google.gson.Gson;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.DeltaLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Imports Delta Lake tables from a ZIP archive to a target location.
 * The ZIP archive should contain all necessary files to replicate the table.
 */
public class DeltaTableImporter {
    private static final Logger LOG = LoggerFactory.getLogger(DeltaTableImporter.class);
    private static final String DELTA_LOG_DIR = "_delta_log";
    private static final int BUFFER_SIZE = 8192;

    private final String zipFilePath;
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
     * @param zipFilePath      The path to the ZIP file containing the Delta table export
     * @param targetPath       The path where the Delta table will be created
     * @param tempDir          The temporary directory to use for extracting files
     * @param overwrite        Whether to overwrite the target table if it exists
     * @param mergeSchema      Whether to merge the schema with the existing table if it exists
     * @param storageProvider  The storage provider to use for file operations
     */
    public DeltaTableImporter(String zipFilePath, String targetPath, String tempDir, 
                             boolean overwrite, boolean mergeSchema,
                             StorageProvider storageProvider) {
        this.zipFilePath = zipFilePath;
        this.targetPath = targetPath;
        this.tempDir = tempDir;
        this.overwrite = overwrite;
        this.mergeSchema = mergeSchema;
        this.storageProvider = storageProvider;
        this.gson = new Gson();
    }

    /**
     * Imports the Delta table from the ZIP file to the target location.
     *
     * @throws IOException If an I/O error occurs
     */
    public void importTable() throws IOException {
        LOG.info("Starting import of Delta table from ZIP {} to {}", zipFilePath, targetPath);

        // Create temporary directory if it doesn't exist
        Files.createDirectories(Paths.get(tempDir));
        
        // Extract the ZIP file to the temporary directory
        extractZipFile();
        
        // Read metadata if available
        readMetadata();
        
        // Initialize Spark
        initializeSpark();
        
        // Validate the extracted Delta table
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
     * Extracts the ZIP file to the temporary directory.
     *
     * @throws IOException If an I/O error occurs
     */
    private void extractZipFile() throws IOException {
        LOG.info("Extracting ZIP file to temporary directory: {}", tempDir);
        
        try (ZipInputStream zipIn = new ZipInputStream(new FileInputStream(zipFilePath))) {
            ZipEntry entry;
            while ((entry = zipIn.getNextEntry()) != null) {
                String filePath = tempDir + File.separator + entry.getName();
                
                if (entry.isDirectory()) {
                    // Create directory if it doesn't exist
                    Files.createDirectories(Paths.get(filePath));
                } else {
                    // Create parent directories if they don't exist
                    Files.createDirectories(Paths.get(filePath).getParent());
                    
                    // Extract file
                    try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath))) {
                        byte[] buffer = new byte[BUFFER_SIZE];
                        int read;
                        while ((read = zipIn.read(buffer)) != -1) {
                            bos.write(buffer, 0, read);
                        }
                    }
                }
                
                zipIn.closeEntry();
            }
        }
        
        LOG.info("ZIP file extracted successfully");
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
                deltaLogFiles.forEach(source -> {
                    try {
                        String fileName = source.getFileName().toString();
                        String destinationPath = targetPath + "/" + DELTA_LOG_DIR + "/" + fileName;
                        LOG.info("Copying Delta log file: {} -> {}", source, destinationPath);
                        storageProvider.uploadFile(source.toString(), destinationPath);
                    } catch (IOException e) {
                        LOG.error("Failed to copy Delta log file: {}", source, e);
                    }
                });
            }
        }
        
        // Copy all other files from temp directory to target
        Files.walk(Paths.get(tempDir))
                .filter(source -> !Files.isDirectory(source))
                .filter(source -> !source.toString().contains("/" + DELTA_LOG_DIR + "/")) // Skip _delta_log files as they were already copied
                .forEach(source -> {
                    try {
                        Path relativePath = Paths.get(tempDir).relativize(source);
                        String destinationPath = targetPath + "/" + relativePath.toString();
                        
                        // Upload the file to the target storage
                        LOG.info("Copying file: {} -> {}", source, destinationPath);
                        storageProvider.uploadFile(source.toString(), destinationPath);
                    } catch (IOException e) {
                        LOG.error("Failed to copy file: {}", source, e);
                    }
                });
        
        LOG.info("Files copied successfully");
    }

    /**
     * Verifies that the imported table is valid and can be read by Spark.
     *
     * @throws IOException If the verification fails
     */
    private void verifyImportedTable() throws IOException {
        LOG.info("Verifying imported Delta table at path: {}", targetPath);
        
        try {
            // Convert path to be compatible with Spark's expectations
            String sparkPath = targetPath;
            if (!sparkPath.startsWith("file:")) {
                sparkPath = "file://" + sparkPath;
            }
            LOG.info("Using Spark path: {}", sparkPath);
            
            // Force Spark to refresh its cache of the Delta table
            spark.sql("REFRESH TABLE delta.`" + sparkPath + "`");
            
            // Try to open the Delta table
            DeltaLog deltaLog = DeltaLog.forTable(spark, sparkPath);
            long version = deltaLog.currentSnapshot().snapshot().version();
            LOG.info("Successfully verified Delta table at version {}", version);
            
            // Try to read the table data
            long rowCount = spark.read().format("delta").load(sparkPath).count();
            LOG.info("Delta table contains {} rows", rowCount);
        } catch (Exception e) {
            LOG.error("Verification error", e);
            throw new IOException("Failed to verify imported Delta table", e);
        }
    }

    /**
     * Reads the metadata.json file if it exists.
     */
    private void readMetadata() {
        Path metadataPath = Paths.get(tempDir, "metadata.json");
        if (Files.exists(metadataPath)) {
            try (Reader reader = Files.newBufferedReader(metadataPath)) {
                metadata = gson.fromJson(reader, ExportMetadata.class);
                LOG.info("Read metadata: {}", metadata);
                LOG.info("Importing Delta table from {} (versions {}-{}), exported at {}", 
                        metadata.getTablePath(), metadata.getFromVersion(), 
                        metadata.getToVersion(), metadata.getExportTimestamp());
            } catch (Exception e) {
                LOG.warn("Failed to read metadata.json: {}", e.getMessage());
            }
        } else {
            LOG.warn("No metadata.json found in the export");
        }
    }
}
