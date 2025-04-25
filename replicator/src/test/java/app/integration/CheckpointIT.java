package app.integration;

import app.common.storage.StorageProvider;
import app.common.storage.StorageProviderFactory;
import app.exporter.DeltaTableExporter;
import app.importer.DeltaTableImporter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.DeltaLog;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for exporting and importing Delta tables with checkpoint files.
 */
public class CheckpointIT extends AbstractIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointIT.class);
    
    @BeforeAll
    public static void setupCheckpoints() {
        // Initialize Spark with configurations to ensure checkpoint generation
        spark = SparkSession.builder()
                .appName("DeltaTableCheckpointTest")
                .master("local[*]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                // Ensure checkpoints are created frequently for testing
                .config("spark.databricks.delta.properties.defaults.checkpointInterval", "5")
                .getOrCreate();
    }
    
    /**
     * Tests the export and import of a Delta table with checkpoint files.
     */
    @Test
    public void testCheckpointExportImport() throws IOException {
        // Create source directory for the Delta table
        Path sourceTablePath = tempDir.resolve("checkpoint_source_table");
        Files.createDirectories(sourceTablePath);
        
        // Create target directory for the import
        Path targetTablePath = tempDir.resolve("checkpoint_target_table");
        Files.createDirectories(targetTablePath);
        
        // Create temp directories for the export/import process
        Path exportTempDir = tempDir.resolve("checkpoint_export_temp");
        Files.createDirectories(exportTempDir);
        Path importTempDir = tempDir.resolve("checkpoint_import_temp");
        Files.createDirectories(importTempDir);
        
        // Create a zip file path for the export
        String zipFilePath = tempDir.resolve("checkpoint_delta_export.zip").toString();
        
        // Create a Delta table with enough operations to generate checkpoints
        createLargeDeltaTableWithCheckpoints(sourceTablePath.toString());
        
        // Verify that checkpoints were created
        Path deltaLogPath = Paths.get(sourceTablePath.toString(), "_delta_log");
        assertTrue(Files.exists(deltaLogPath), "Delta log directory should exist");
        
        // Check for checkpoint files
        boolean hasCheckpoints = false;
        try (var files = Files.list(deltaLogPath)) {
            hasCheckpoints = files
                    .anyMatch(p -> p.toString().endsWith(".checkpoint.parquet"));
        }
        assertTrue(hasCheckpoints, "Checkpoint files should have been created");
        
        // Get the source table version
        DeltaLog sourceLog = DeltaLog.forTable(spark, sourceTablePath.toString());
        long sourceVersion = sourceLog.currentSnapshot().snapshot().version();
        LOG.info("Source table created with version: {}", sourceVersion);
        
        // Count files in the _delta_log directory before export
        Map<String, Long> sourceFileMap = countFileTypesByExtension(deltaLogPath);
        LOG.info("Source delta log files: {}", sourceFileMap);
        
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
        Path targetDeltaLogPath = Paths.get(targetTablePath.toString(), "_delta_log");
        assertTrue(Files.exists(targetDeltaLogPath), "Target Delta log directory should exist");
        
        // Count files in the target _delta_log directory
        Map<String, Long> targetFileMap = countFileTypesByExtension(targetDeltaLogPath);
        LOG.info("Target delta log files: {}", targetFileMap);
        
        // Verify that checkpoint files were imported
        assertTrue(targetFileMap.getOrDefault(".checkpoint.parquet", 0L) > 0, 
                "Checkpoint files should have been imported");
        
        // Verify file counts match between source and target
        assertEquals(sourceFileMap.getOrDefault(".json", 0L), 
                    targetFileMap.getOrDefault(".json", 0L), 
                    "Number of JSON log files should match");
        assertEquals(sourceFileMap.getOrDefault(".checkpoint.parquet", 0L), 
                    targetFileMap.getOrDefault(".checkpoint.parquet", 0L), 
                    "Number of checkpoint files should match");
        
        // Verify the data matches
        verifyImportedTable(sourceTablePath.toString(), targetTablePath.toString());
        
        // Verify the table versions
        DeltaLog targetLog = DeltaLog.forTable(spark, targetTablePath.toString());
        long targetVersion = targetLog.currentSnapshot().snapshot().version();
        LOG.info("Target table imported with version: {}", targetVersion);
        assertEquals(sourceVersion, targetVersion, "Source and target table versions should match");
    }
    
    /**
     * Creates a large Delta table with multiple operations to trigger checkpoint creation.
     *
     * @param tablePath Path where the Delta table will be created
     */
    private void createLargeDeltaTableWithCheckpoints(String tablePath) {
        LOG.info("Creating large Delta table with multiple operations to trigger checkpoints at {}", tablePath);
        
        // Create a larger dataset - this will be our base table
        Dataset<Row> df = spark.range(1, 1000)
                .toDF("id")
                .withColumn("value", concat(lit("value_"), col("id")))
                .withColumn("category", expr("CASE WHEN id % 10 = 0 THEN 'A' WHEN id % 5 = 0 THEN 'B' ELSE 'C' END"))
                .withColumn("created_at", current_timestamp());
        
        // Write as Delta table
        df.write()
                .format("delta")
                .mode("overwrite")
                .save(tablePath);
        LOG.info("Created initial Delta table with {} rows", df.count());
        
        // Perform multiple operations to trigger checkpoint creation
        // Operation 1: Add a new column
        spark.read().format("delta").load(tablePath)
                .withColumn("updated_at", current_timestamp())
                .write()
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(tablePath);
        LOG.info("Applied operation 1: Added updated_at column");
        
        // Operation 2: Add more rows in batches
        for (int batch = 0; batch < 10; batch++) {
            int start = 1000 + (batch * 1000);
            int end = start + 1000;
            Dataset<Row> moreData = spark.range(start, end)
                    .toDF("id")
                    .withColumn("value", concat(lit("value_"), col("id")))
                    .withColumn("category", expr("CASE WHEN id % 10 = 0 THEN 'A' WHEN id % 5 = 0 THEN 'B' ELSE 'C' END"))
                    .withColumn("created_at", current_timestamp())
                    .withColumn("updated_at", current_timestamp());
            
            moreData.write()
                    .format("delta")
                    .mode("append")
                    .save(tablePath);
            LOG.info("Applied operation 2.{}: Added {} more rows (total batch {})", batch, 1000, batch + 1);
        }
        
        // Operation 3: Perform updates
        spark.sql("UPDATE delta.`" + tablePath + "` SET value = 'updated_A' WHERE category = 'A'");
        LOG.info("Applied operation 3.1: Updated rows with category A");
        
        spark.sql("UPDATE delta.`" + tablePath + "` SET value = 'updated_B' WHERE category = 'B'");
        LOG.info("Applied operation 3.2: Updated rows with category B");
        
        // Operation 4: Delete some data
        spark.sql("DELETE FROM delta.`" + tablePath + "` WHERE id % 50 = 0");
        LOG.info("Applied operation 4: Deleted rows where id is multiple of 50");
        
        // Operation 5: Add more columns
        spark.read().format("delta").load(tablePath)
                .withColumn("is_active", lit(true))
                .withColumn("score", expr("CAST(RAND() * 100 AS INT)"))
                .write()
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(tablePath);
        LOG.info("Applied operation 5: Added is_active and score columns");
        
        // Operation 6: More updates to trigger more commits
        spark.sql("UPDATE delta.`" + tablePath + "` SET is_active = false WHERE id % 25 = 0");
        LOG.info("Applied operation 6: Updated is_active flag for some rows");
        
        // Verify final row count and schema
        Dataset<Row> finalTable = spark.read().format("delta").load(tablePath);
        LOG.info("Final Delta table has {} rows and {} columns", finalTable.count(), finalTable.columns().length);
        LOG.info("Final schema: {}", finalTable.schema().treeString());
        
        // Force checkpoint creation (though it should have happened automatically with our config)
        //spark.sql("VACUUM delta.`" + tablePath + "` RETAIN 100 HOURS");
        //LOG.info("Vacuum completed to finalize the Delta table");
    }
    
    /**
     * Counts files in the given directory by their extension.
     *
     * @param dirPath The directory path to count files in
     * @return A map of extension to count
     */
    private Map<String, Long> countFileTypesByExtension(Path dirPath) throws IOException {
        try (var files = Files.list(dirPath)) {
            return files
                    .filter(Files::isRegularFile)
                    .collect(Collectors.groupingBy(
                            path -> {
                                String name = path.getFileName().toString();
                                if (name.endsWith(".checkpoint.parquet")) {
                                    return ".checkpoint.parquet";
                                } else if (name.endsWith(".json")) {
                                    return ".json";
                                } else {
                                    int lastDotIndex = path.toString().lastIndexOf('.');
                                    return lastDotIndex == -1 ? "(no extension)" : path.toString().substring(lastDotIndex);
                                }
                            },
                            Collectors.counting()
                    ));
        }
    }
}
