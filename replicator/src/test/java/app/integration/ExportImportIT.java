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
import java.nio.file.Paths;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for the combined export and import process.
 */
public class ExportImportIT extends AbstractIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(ExportImportIT.class);
    
    /**
     * Tests the full export and import cycle with a simple Delta table.
     */
    @Test
    public void testExportImportCycle() throws IOException {
        // Create source directory for the Delta table
        Path sourceTablePath = tempDir.resolve("source_table");
        Files.createDirectories(sourceTablePath);
        
        // Create target directory for the import
        Path targetTablePath = tempDir.resolve("target_table");
        Files.createDirectories(targetTablePath);
        
        // Create temp directory for the export/import process
        Path exportTempDir = tempDir.resolve("export_temp");
        Files.createDirectories(exportTempDir);
        Path importTempDir = tempDir.resolve("import_temp");
        Files.createDirectories(importTempDir);
        
        // Create a zip file path for the export
        String zipFilePath = tempDir.resolve("delta_export.zip").toString();
        
        // Create a simple Delta table with test data
        createTestDeltaTable(sourceTablePath.toString());
        
        // Get the source table version
        DeltaLog sourceLog = DeltaLog.forTable(spark, sourceTablePath.toString());
        long sourceVersion = sourceLog.currentSnapshot().snapshot().version();
        
        LOG.info("Source table created with version: {}", sourceVersion);
        
        // Create storage provider for the source table
        StorageProvider sourceStorageProvider = StorageProviderFactory.createProvider(
                "file://" + sourceTablePath.toString());
        
        // Export the Delta table
        DeltaTableExporter exporter = new DeltaTableExporter(
                sourceTablePath.toString(), 0, zipFilePath, exportTempDir.toString(), sourceStorageProvider);
        
        String exportedZipPath = exporter.export();
        LOG.info("Exported Delta table to: {}", exportedZipPath);
        
        // Verify the zip file was created
        File zipFile = new File(exportedZipPath);
        assertTrue(zipFile.exists(), "Export ZIP file should exist");
        assertTrue(zipFile.length() > 0, "Export ZIP file should not be empty");
        
        // Create storage provider for the target table
        StorageProvider targetStorageProvider = StorageProviderFactory.createProvider(
                "file://" + targetTablePath.toString());
        
        // Import the Delta table
        DeltaTableImporter importer = new DeltaTableImporter(
                zipFilePath, targetTablePath.toString(), importTempDir.toString(), 
                true, false, targetStorageProvider);
        
        importer.importTable();
        LOG.info("Imported Delta table to: {}", targetTablePath);
        
        // Verify the import was successful
        verifyImportedTable(sourceTablePath.toString(), targetTablePath.toString());
        
        // Verify the table versions
        DeltaLog targetLog = DeltaLog.forTable(spark, targetTablePath.toString());
        long targetVersion = targetLog.currentSnapshot().snapshot().version();

        LOG.info("Target table imported with version: {}", targetVersion);
        assertEquals(sourceVersion, targetVersion, "Source and target table versions should match");
    }
    
    /**
     * Tests the full export and import cycle with a simple Delta table.
     */
    @Test
    public void manualTest() throws IOException {
        // Create source directory for the Delta table
        Path sourceTablePath = Paths.get("/tmp/source_table");
        Files.createDirectories(sourceTablePath);

        // Create target directory for the import
        Path targetTablePath = Paths.get("/tmp/target_table");
        Files.createDirectories(targetTablePath);

        // Create temp directory for the export/import process
        Path exportTempDir = Paths.get("/tmp/export_temp");
        Files.createDirectories(exportTempDir);
        Path importTempDir = Paths.get("/tmp/import_temp");
        Files.createDirectories(importTempDir);

        // Create a zip file path for the export
        String zipFilePath = Paths.get("/tmp/delta_export.zip").toString();

        // Create a simple Delta table with test data
        createTestDeltaTable(sourceTablePath.toString());

        // Get the source table version
        DeltaLog sourceLog = DeltaLog.forTable(spark, sourceTablePath.toString());
        long sourceVersion = sourceLog.currentSnapshot().snapshot().version();

        LOG.info("Source table created with version: {}", sourceVersion);

        // Create storage provider for the source table
        StorageProvider sourceStorageProvider = StorageProviderFactory.createProvider(
                "file://" + sourceTablePath.toString());

        // Export the Delta table
        DeltaTableExporter exporter = new DeltaTableExporter(
                sourceTablePath.toString(), 0, zipFilePath, exportTempDir.toString(), sourceStorageProvider);

        String exportedZipPath = exporter.export();
        LOG.info("Exported Delta table to: {}", exportedZipPath);

        // Verify the zip file was created
        File zipFile = new File(exportedZipPath);
        assertTrue(zipFile.exists(), "Export ZIP file should exist");
        assertTrue(zipFile.length() > 0, "Export ZIP file should not be empty");

        // Create storage provider for the target table
        StorageProvider targetStorageProvider = StorageProviderFactory.createProvider(
                "file://" + targetTablePath.toString());

        // Import the Delta table
        DeltaTableImporter importer = new DeltaTableImporter(
                zipFilePath, targetTablePath.toString(), importTempDir.toString(),
                true, false, targetStorageProvider);

        importer.importTable();
        LOG.info("Imported Delta table to: {}", targetTablePath);

        // Verify the import was successful
        verifyImportedTable(sourceTablePath.toString(), targetTablePath.toString());

        // Verify the table versions
        DeltaLog targetLog = DeltaLog.forTable(spark, targetTablePath.toString());
        long targetVersion = targetLog.currentSnapshot().snapshot().version();

        LOG.info("Target table imported with version: {}", targetVersion);
        assertEquals(sourceVersion, targetVersion, "Source and target table versions should match");
    }
    
    /**
     * Creates a test Delta table with sample data.
     *
     * @param tablePath Path where the Delta table will be created
     */
    private void createTestDeltaTable(String tablePath) {
        // Create a simple dataset
        Dataset<Row> df = spark.range(1, 100)
                .toDF("id")
                .withColumn("value", concat(lit("value_"), col("id")))
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
        
        // Update #2: Add more rows
        Dataset<Row> moreData = spark.range(100, 200)
                .toDF("id")
                .withColumn("value", concat(lit("value_"), col("id")))
                .withColumn("uuid", expr("uuid()"))
                .withColumn("timestamp", current_timestamp());
        
        moreData.write()
                .format("delta")
                .mode("append")
                .save(tablePath);
        
        // Update #3: Update some data
        spark.sql("UPDATE delta.`" + tablePath + "` SET value = 'updated_value' WHERE id % 10 = 0");
    }
}
