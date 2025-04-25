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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration tests for incremental export and import operations with partitioned Delta tables.
 */
public class PartitionedIncrementalExportImportIT extends AbstractIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionedIncrementalExportImportIT.class);
    
    /**
     * Tests incremental export and import with multiple rounds of updates on a partitioned Delta table.
     * <p>
     * This test workflow:
     * 1. Creates a partitioned source table with initial records
     * 2. Exports and imports to a target table
     * 3. Adds more commits to the source table affecting different partitions
     * 4. Exports only the new commits
     * 5. Applies them to the target and verifies
     * 6. Adds schema changes and more commits to the source
     * 7. Exports and applies those new commits
     * 8. Verifies final state
     */
    @Test
    public void testPartitionedIncrementalExportImport() throws IOException {
        // Create source directory for the Delta table
        Path sourceTablePath = tempDir.resolve("partitioned_incremental_source_table");
        Files.createDirectories(sourceTablePath);
        
        // Create target directory for the import
        Path targetTablePath = tempDir.resolve("partitioned_incremental_target_table");
        Files.createDirectories(targetTablePath);
        
        // Create temp directories for the export/import process
        Path exportTempDir = tempDir.resolve("partitioned_incremental_export_temp");
        Files.createDirectories(exportTempDir);
        Path importTempDir = tempDir.resolve("partitioned_incremental_import_temp");
        Files.createDirectories(importTempDir);
        
        LOG.info("========== PHASE 1: INITIAL SETUP WITH PARTITIONING ==========");
        
        // STEP 1: Create initial partitioned Delta table with test data
        LOG.info("Creating initial partitioned Delta table");
        Dataset<Row> initialData = spark.range(1, 1000)
                .toDF("id")
                .withColumn("value", concat(lit("value_"), col("id")))
                .withColumn("category", expr("CASE WHEN id % 10 = 0 THEN 'A' WHEN id % 5 = 0 THEN 'B' ELSE 'C' END"))
                .withColumn("year", expr("2023"))
                .withColumn("month", expr("CAST(((id % 12) + 1) AS INT)"))
                .withColumn("created_at", current_timestamp());
        
        // Write with partitioning by year and month
        initialData.write()
                .format("delta")
                .partitionBy("year", "month")
                .mode("overwrite")
                .save(sourceTablePath.toString());
        
        // Verify initial row count
        Dataset<Row> sourceTable = spark.read().format("delta").load(sourceTablePath.toString());
        long initialCount = sourceTable.count();
        LOG.info("Initial partitioned source table created with {} rows", initialCount);
        
        // Verify the partitioning
        LOG.info("Verifying partition structure");
        Dataset<Row> partitionInfo = spark.sql("DESCRIBE DETAIL delta.`" + sourceTablePath + "`");
        partitionInfo.select("numFiles", "partitionColumns").show(false);
        
        // Get initial source table version
        DeltaLog sourceLog = DeltaLog.forTable(spark, sourceTablePath.toString());
        long sourceVersion = sourceLog.currentSnapshot().snapshot().version();
        LOG.info("Initial partitioned source table version: {}", sourceVersion);
        
        // STEP 2: Export the initial Delta table
        String initialZipPath = tempDir.resolve("initial_partitioned_delta_export.zip").toString();
        
        StorageProvider sourceStorageProvider = StorageProviderFactory.createProvider(
                "file://" + sourceTablePath);
        
        DeltaTableExporter initialExporter = new DeltaTableExporter(
                sourceTablePath.toString(), 0, initialZipPath, exportTempDir.toString(), sourceStorageProvider);
        
        String exportedInitialZipPath = initialExporter.export();
        LOG.info("Exported initial partitioned Delta table to: {}", exportedInitialZipPath);
        
        // STEP 3: Import the initial Delta table to target
        StorageProvider targetStorageProvider = StorageProviderFactory.createProvider(
                "file://" + targetTablePath);
        
        DeltaTableImporter initialImporter = new DeltaTableImporter(
                initialZipPath, targetTablePath.toString(), importTempDir.toString(), 
                true, false, targetStorageProvider);
        
        initialImporter.importTable();
        LOG.info("Imported initial partitioned Delta table to: {}", targetTablePath);
        
        // Verify initial import was successful
        verifyImportedTable(sourceTablePath.toString(), targetTablePath.toString());
        
        // Verify the target table is also partitioned
        Dataset<Row> targetPartitionInfo = spark.sql("DESCRIBE DETAIL delta.`" + targetTablePath + "`");
        LOG.info("Target table partition info:");
        targetPartitionInfo.select("numFiles", "partitionColumns").show(false);
        
        // Get target table version after initial import
        DeltaLog targetLog = DeltaLog.forTable(spark, targetTablePath.toString());
        long targetVersion = targetLog.currentSnapshot().snapshot().version();
        LOG.info("Target table imported with version: {}", targetVersion);
        assertEquals(sourceVersion, targetVersion, "Initial source and target table versions should match");
        
        LOG.info("========== PHASE 2: UPDATE DIFFERENT PARTITIONS ==========");
        
        // STEP 4: Perform first round of updates on source table, focusing on specific partitions
        LOG.info("Performing first update on specific partitions of the source table");
        
        // Add more rows to 2024 year partition (new partition)
        Dataset<Row> update1Data = spark.range(1000, 2000)
                .toDF("id")
                .withColumn("value", concat(lit("update1_"), col("id")))
                .withColumn("category", expr("CASE WHEN id % 10 = 0 THEN 'A' WHEN id % 5 = 0 THEN 'B' ELSE 'C' END"))
                .withColumn("year", expr("2024"))
                .withColumn("month", expr("CAST(((id % 12) + 1) AS INT)"))
                .withColumn("created_at", current_timestamp());
        
        update1Data.write()
                .format("delta")
                .partitionBy("year", "month")
                .mode("append")
                .save(sourceTablePath.toString());
        
        // Update some existing rows in specific partitions (year=2023, month=1)
        spark.sql("UPDATE delta.`" + sourceTablePath + "` SET value = 'updated_first_jan_2023' WHERE year = 2023 AND month = 1");
        
        // Get source table version after first update
        sourceLog = DeltaLog.forTable(spark, sourceTablePath.toString());
        long sourceVersionAfterUpdate1 = sourceLog.currentSnapshot().snapshot().version();
        LOG.info("Source table version after first update: {}", sourceVersionAfterUpdate1);
        
        // STEP 5: Perform incremental export (from version after initial import to latest)
        String update1ZipPath = tempDir.resolve("update1_partitioned_delta_export.zip").toString();
        
        DeltaTableExporter update1Exporter = new DeltaTableExporter(
                sourceTablePath.toString(), targetVersion + 1, update1ZipPath, exportTempDir.toString(), sourceStorageProvider);
        
        String exportedUpdate1ZipPath = update1Exporter.export();
        LOG.info("Exported incremental update 1 to: {}", exportedUpdate1ZipPath);
        
        // STEP 6: Apply incremental update to target
        DeltaTableImporter update1Importer = new DeltaTableImporter(
                update1ZipPath, targetTablePath.toString(), importTempDir.toString(), 
                false, true, targetStorageProvider);
        
        update1Importer.importTable();
        LOG.info("Applied incremental update 1 to target table");
        
        // Verify incremental update was successful
        verifyImportedTable(sourceTablePath.toString(), targetTablePath.toString());
        
        // Check partition structure after update
        LOG.info("Verifying partition structure after update");
        spark.sql("DESCRIBE DETAIL delta.`" + targetTablePath + "`").select("numFiles", "partitionColumns").show(false);
        
        // Get target table version after incremental update
        targetLog = DeltaLog.forTable(spark, targetTablePath.toString());
        long targetVersionAfterUpdate1 = targetLog.currentSnapshot().snapshot().version();
        LOG.info("Target table version after incremental update 1: {}", targetVersionAfterUpdate1);
        assertEquals(sourceVersionAfterUpdate1, targetVersionAfterUpdate1,
                "Source and target versions should match after first incremental update");
        
        LOG.info("========== PHASE 3: SCHEMA CHANGE AND MORE PARTITION UPDATES ==========");
        
        // STEP 7: Perform second round of updates with schema changes and affecting different partitions
        LOG.info("Performing second update with schema change and new partitions");
        
        // Add new columns to the schema
        Dataset<Row> sourceTableWithNewSchema = spark.read().format("delta").load(sourceTablePath.toString())
                .withColumn("updated_at", current_timestamp())
                .withColumn("data_quality", 
                    expr("CASE year WHEN 2023 THEN 'HISTORICAL' WHEN 2024 THEN 'CURRENT' ELSE 'UNKNOWN' END"));
        
        sourceTableWithNewSchema.write()
                .format("delta")
                .partitionBy("year", "month")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(sourceTablePath.toString());
        
        // Add more data with the new schema and a new year partition
        Dataset<Row> update2Data = spark.range(2000, 3000)
                .toDF("id")
                .withColumn("value", concat(lit("update2_"), col("id")))
                .withColumn("category", expr("CASE WHEN id % 10 = 0 THEN 'A' WHEN id % 5 = 0 THEN 'B' ELSE 'C' END"))
                .withColumn("year", expr("2025"))
                .withColumn("month", expr("CAST(((id % 12) + 1) AS INT)"))
                .withColumn("created_at", current_timestamp())
                .withColumn("updated_at", current_timestamp())
                .withColumn("data_quality", lit("FUTURE"));
        
        update2Data.write()
                .format("delta")
                .partitionBy("year", "month")
                .mode("append")
                .save(sourceTablePath.toString());
        
        // Delete entire partition (year=2023, month=12)
        spark.sql("DELETE FROM delta.`" + sourceTablePath + "` WHERE year = 2023 AND month = 12");
        
        // Get source table version after second update
        sourceLog = DeltaLog.forTable(spark, sourceTablePath.toString());
        long sourceVersionAfterUpdate2 = sourceLog.currentSnapshot().snapshot().version();
        LOG.info("Source table version after second update: {}", sourceVersionAfterUpdate2);
        
        // STEP 8: Perform second incremental export
        String update2ZipPath = tempDir.resolve("update2_partitioned_delta_export.zip").toString();
        
        DeltaTableExporter update2Exporter = new DeltaTableExporter(
                sourceTablePath.toString(), targetVersionAfterUpdate1 + 1, update2ZipPath, exportTempDir.toString(), sourceStorageProvider);
        
        String exportedUpdate2ZipPath = update2Exporter.export();
        LOG.info("Exported incremental update 2 to: {}", exportedUpdate2ZipPath);
        
        // STEP 9: Apply second incremental update to target
        DeltaTableImporter update2Importer = new DeltaTableImporter(
                update2ZipPath, targetTablePath.toString(), importTempDir.toString(), 
                false, true, targetStorageProvider);
        
        update2Importer.importTable();
        LOG.info("Applied incremental update 2 to target table");
        
        // STEP 10: Final verification
        LOG.info("Performing final verification");
        verifyImportedTable(sourceTablePath.toString(), targetTablePath.toString());
        
        // Get final target table version
        targetLog = DeltaLog.forTable(spark, targetTablePath.toString());
        long finalTargetVersion = targetLog.currentSnapshot().snapshot().version();
        LOG.info("Final target table version: {}", finalTargetVersion);
        assertEquals(sourceVersionAfterUpdate2, finalTargetVersion,
                "Source and target versions should match after second incremental update");
        
        // Verify row count, schema, and partition structure
        Dataset<Row> finalSourceTable = spark.read().format("delta").load(sourceTablePath.toString());
        Dataset<Row> finalTargetTable = spark.read().format("delta").load(targetTablePath.toString());
        LOG.info("Final source table has {} rows and {} columns", finalSourceTable.count(), finalSourceTable.columns().length);
        LOG.info("Final target table has {} rows and {} columns", finalTargetTable.count(), finalTargetTable.columns().length);
        
        // Check distribution of data across partitions
        LOG.info("Checking distribution of data across partitions in source:");
        spark.sql("SELECT year, month, COUNT(*) as count FROM delta.`" + sourceTablePath + "` GROUP BY year, month ORDER BY year, month").show(50);
        
        LOG.info("Checking distribution of data across partitions in target:");
        spark.sql("SELECT year, month, COUNT(*) as count FROM delta.`" + targetTablePath + "` GROUP BY year, month ORDER BY year, month").show(50);
    }
}
