package app.integration;

import app.common.storage.StorageProvider;
import app.common.storage.StorageProviderFactory;
import app.exporter.DeltaTableExporter;
import app.importer.DeltaTableImporter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for ZIP export and import functionality.
 */
public class MultiVolumeExportIT extends AbstractIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(MultiVolumeExportIT.class);
    
    /**
     * Tests exporting to ZIP archive.
     */
    @Test
    public void testZipExport() throws IOException {
        // Create source directory for the Delta table
        Path sourceTablePath = tempDir.resolve("source_table_zip");
        Files.createDirectories(sourceTablePath);
        
        // Create target directory for the import
        Path targetTablePath = tempDir.resolve("target_table_zip");
        Files.createDirectories(targetTablePath);
        
        // Create temp directory for the export/import process
        Path exportTempDir = tempDir.resolve("export_temp_zip");
        Files.createDirectories(exportTempDir);
        Path importTempDir = tempDir.resolve("import_temp_zip");
        Files.createDirectories(importTempDir);
        
        // Create a ZIP file path for the export
        String archiveBasePath = tempDir.resolve("delta_export_zip").toString();
        
        // Create a larger Delta table with test data
        createLargerTestDeltaTable(sourceTablePath.toString());
        
        // Get the source table version
        DeltaLog sourceLog = DeltaLog.forTable(spark, sourceTablePath.toString());
        long sourceVersion = sourceLog.currentSnapshot().snapshot().version();
        
        LOG.info("Source table created with version: {}", sourceVersion);
        
        // Create storage provider for the source table
        StorageProvider sourceStorageProvider = StorageProviderFactory.createProvider(
                "file://" + sourceTablePath.toString());
        
        // Export the Delta table to a ZIP archive
        DeltaTableExporter exporter = new DeltaTableExporter(
                sourceTablePath.toString(), 0, archiveBasePath + ".zip", 
                exportTempDir.toString(), sourceStorageProvider);
        
        String exportedArchivePath = exporter.export();
        LOG.info("Exported Delta table to: {}", exportedArchivePath);
        
        // Check the entire temp directory for the ZIP file
        File dir = tempDir.toFile();
        LOG.info("Looking for ZIP file in temp directory: {}", dir.getAbsolutePath());
        
        File[] archiveFiles = dir.listFiles(file -> file.getName().endsWith(".zip"));
        
        if (archiveFiles != null && archiveFiles.length > 0) {
            LOG.info("Found {} archive files:", archiveFiles.length);
            for (File file : archiveFiles) {
                LOG.info("  - {}: {} bytes", file.getName(), file.length());
            }
            
            // Verify that the archive file exists and has content
            assertTrue(archiveFiles.length > 0, "ZIP archive file should exist");
            assertTrue(archiveFiles[0].length() > 0, "Archive file should not be empty");
            
            // Use the first ZIP file
            File zipFile = archiveFiles[0];
            LOG.info("Using ZIP archive: {}", zipFile.getAbsolutePath());
            
            try {
                // Create storage provider for the target table
                StorageProvider targetStorageProvider = StorageProviderFactory.createProvider(
                        "file://" + targetTablePath.toString());
                
                // Import the Delta table
                DeltaTableImporter importer = new DeltaTableImporter(
                        zipFile.getAbsolutePath(), targetTablePath.toString(), importTempDir.toString(), 
                        true, false, targetStorageProvider);
                
                importer.importTable();
                LOG.info("Imported Delta table to: {}", targetTablePath);
                
                // Verify the import was successful
                verifyImportedTable(sourceTablePath.toString(), targetTablePath.toString());
                
                // Verify the table versions
                DeltaLog targetLog = DeltaLog.forTable(spark, targetTablePath.toString());
                long targetVersion = targetLog.snapshot().version();
                
                LOG.info("Target table imported with version: {}", targetVersion);
                assertEquals(sourceVersion, targetVersion, "Source and target table versions should match");
                
                LOG.info("Successfully verified end-to-end export and import process");
            } catch (Exception e) {
                LOG.error("Error during import process", e);
                fail("Import process failed: " + e.getMessage());
            }
        } else {
            fail("No ZIP archive files found in directory: " + dir.getAbsolutePath());
        }
    }
    
    /**
     * Creates a larger test Delta table with more data.
     *
     * @param tablePath Path where the Delta table will be created
     */
    private void createLargerTestDeltaTable(String tablePath) {
        // Create a dataset with more rows and larger values
        Dataset<Row> df = spark.range(1, 10000)  // 10K rows
                .toDF("id")
                .withColumn("value", concat(
                    lit("value_"), col("id"), 
                    lit(generateRepeatedString('X', 1024))  // Add 1KB of data per row
                ))
                .withColumn("uuid", expr("uuid()"));
        
        // Write as Delta table
        df.write()
                .format("delta")
                .mode("overwrite")
                .save(tablePath);
        
        // Make a few updates to create multiple versions
        // Update #1: Add a new column
        spark.read().format("delta").load(tablePath)
                .withColumn("timestamp", current_timestamp())
                .write()
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(tablePath);
        
        // Update #2: Add more rows with large data
        Dataset<Row> moreData = spark.range(10000, 15000)  // 5K more rows
                .toDF("id")
                .withColumn("value", concat(
                    lit("value_"), col("id"), 
                    lit(generateRepeatedString('Y', 1024))  // Add 1KB of data per row
                ))
                .withColumn("uuid", expr("uuid()"))
                .withColumn("timestamp", current_timestamp());
        
        moreData.write()
                .format("delta")
                .mode("append")
                .save(tablePath);
        
        // Update #3: Update some data
        spark.sql("UPDATE delta.`" + tablePath + "` SET value = concat(value, '_updated') WHERE id <= 100");
        
        LOG.info("Created larger test Delta table with multiple versions at: {}", tablePath);
    }
    
    /**
     * Generates a string by repeating a character multiple times.
     *
     * @param c The character to repeat
     * @param count Number of times to repeat the character
     * @return A string consisting of the character repeated count times
     */
    private String generateRepeatedString(char c, int count) {
        StringBuilder sb = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            sb.append(c);
        }
        return sb.toString();
    }
}
