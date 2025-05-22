package app.integration;

import app.common.Utils;
import app.common.storage.StorageProvider;
import app.common.storage.StorageProviderFactory;
import app.exporter.DeltaTableExporter;
import app.importer.DeltaTableImporter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.DeltaLog;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for exporting and importing Delta tables with checkpoint files.
 */
public class CheckpointIT extends AbstractIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(CheckpointIT.class);
    
    @Override
    protected SparkSession.Builder customizeSparkSession(SparkSession.Builder builder) {
        // Ensure checkpoints are created frequently for testing
        builder.config("spark.databricks.delta.properties.defaults.checkpointInterval", "5");
        return builder;
    }

    /**
     * Tests the export and import of a Delta table with checkpoint files.
     */
    @Test
    public void testCheckpointExportImport() throws IOException {

        // Create a Delta table with enough operations to generate checkpoints
        createLargeDeltaTableWithCheckpoints(sourceTablePath);

        // Check for checkpoint files
        StorageProvider sourceStorageProvider = StorageProviderFactory.createProvider(sourceTablePath, getS3Settings());
        var sourceDeltaLogFiles = sourceStorageProvider.listFiles(Utils.getDeltaLogPath(sourceTablePath), ".*");
        boolean hasCheckpoints = sourceDeltaLogFiles.stream().anyMatch(p -> p.toString().endsWith(".checkpoint.parquet"));
        assertTrue(hasCheckpoints, "Checkpoint files should have been created");
        
        // Get the source table version
        DeltaLog sourceLog = DeltaLog.forTable(spark, sourceTablePath.toString());
        long sourceVersion = sourceLog.currentSnapshot().snapshot().version();
        LOG.info("Source table created with version: {}", sourceVersion);
        
        // Count files in the _delta_log directory before export
        Map<String, Long> sourceFileMap = countFileTypesByExtension(sourceDeltaLogFiles);
        LOG.info("Source delta log files: {}", sourceFileMap);
        
        // Export the Delta table
        DeltaTableExporter exporter = new DeltaTableExporter(
                sourceTablePath.toString(), 0, zipFilesPath.toString(), exportTempDir.toString(), getS3Settings());
        
        String exportedZipPath = exporter.export();
        LOG.info("Exported Delta table to: {}", exportedZipPath);
        
        // Verify the zip file was created
        File zipFile = new File(exportedZipPath);
        assertTrue(zipFile.exists(), "Export ZIP file should exist");
        assertTrue(zipFile.length() > 0, "Export ZIP file should not be empty");
        
        // Import the Delta table
        DeltaTableImporter importer = new DeltaTableImporter(
                exportedZipPath, targetTablePath.toString(), importTempDir.toString(),
                true, false, getS3Settings());
        
        importer.importTable();
        LOG.info("Imported Delta table to: {}", targetTablePath);

        // Check for checkpoint files
        StorageProvider targetStorageProvider = StorageProviderFactory.createProvider(targetTablePath, getS3Settings());
        var targetDeltaLogFiles = targetStorageProvider.listFiles(Utils.getDeltaLogPath(targetTablePath), ".*");

        // Count files in the target _delta_log directory
        Map<String, Long> targetFileMap = countFileTypesByExtension(targetDeltaLogFiles);
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
}
