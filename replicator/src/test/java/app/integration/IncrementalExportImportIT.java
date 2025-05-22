package app.integration;

import app.exporter.DeltaTableExporter;
import app.importer.DeltaTableImporter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.delta.DeltaLog;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration tests for incremental export and import operations.
 */
public class IncrementalExportImportIT extends AbstractIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(IncrementalExportImportIT.class);
    
    /**
     * Tests incremental export and import with multiple rounds of updates.
     * <p>
     * This test workflow:
     * 1. Creates a source table with initial records
     * 2. Exports and imports to a target table
     * 3. Adds more commits to the source table
     * 4. Exports only the new commits
     * 5. Applies them to the target and verifies
     * 6. Adds even more commits to the source
     * 7. Exports and applies those new commits
     * 8. Verifies final state
     */
    @Test
    public void testIncrementalExportImport() throws IOException {
        
        LOG.info("========== PHASE 1: INITIAL SETUP ==========");
        
        // STEP 1: Create initial Delta table with test data
        LOG.info("Creating initial Delta table");
        Dataset<Row> initialData = spark.range(1, 1000)
                .toDF("id")
                .withColumn("value", concat(lit("value_"), col("id")))
                .withColumn("category", expr("CASE WHEN id % 10 = 0 THEN 'A' WHEN id % 5 = 0 THEN 'B' ELSE 'C' END"))
                .withColumn("created_at", current_timestamp());
        
        initialData.write()
                .format("delta")
                .mode("overwrite")
                .save(sourceTablePath);
        
        // Verify initial row count
        Dataset<Row> sourceTable = spark.read().format("delta").load(sourceTablePath);
        long initialCount = sourceTable.count();
        LOG.info("Initial source table created with {} rows", initialCount);
        
        // Get initial source table version
        DeltaLog sourceLog = DeltaLog.forTable(spark, sourceTablePath);
        long sourceVersion = sourceLog.currentSnapshot().snapshot().version();
        LOG.info("Initial source table version: {}", sourceVersion);
        
        // STEP 2: Export the initial Delta table
        DeltaTableExporter initialExporter = new DeltaTableExporter(
                sourceTablePath, 0, zipFilesPath.toString(), exportTempDir.toString(), getS3Settings());
        
        String exportedInitialZipPath = initialExporter.export();
        LOG.info("Exported initial Delta table to: {}", exportedInitialZipPath);
        
        // STEP 3: Import the initial Delta table to target
        
        DeltaTableImporter initialImporter = new DeltaTableImporter(
                exportedInitialZipPath, targetTablePath, importTempDir.toString(),
                true, false, getS3Settings());
        
        initialImporter.importTable();
        LOG.info("Imported initial Delta table to: {}", targetTablePath);
        
        // Verify initial import was successful
        verifyImportedTable(sourceTablePath, targetTablePath);
        
        // Get target table version after initial import
        DeltaLog targetLog = DeltaLog.forTable(spark, targetTablePath);
        long targetVersion = targetLog.currentSnapshot().snapshot().version();
        LOG.info("Target table imported with version: {}", targetVersion);
        assertEquals(sourceVersion, targetVersion, "Initial source and target table versions should match");
        
        LOG.info("========== PHASE 2: FIRST UPDATE ==========");
        
        // STEP 4: Perform first round of updates on source table
        LOG.info("Performing first update on source table");
        
        // Add more rows
        Dataset<Row> update1Data = spark.range(1000, 2000)
                .toDF("id")
                .withColumn("value", concat(lit("update1_"), col("id")))
                .withColumn("category", expr("CASE WHEN id % 10 = 0 THEN 'A' WHEN id % 5 = 0 THEN 'B' ELSE 'C' END"))
                .withColumn("created_at", current_timestamp());
        
        update1Data.write()
                .format("delta")
                .mode("append")
                .save(sourceTablePath);
        
        // Update some existing rows
        spark.sql("UPDATE delta.`" + sourceTablePath + "` SET value = 'updated_first_A' WHERE category = 'A' AND id < 1000");
        
        // Get source table version after first update
        sourceLog = DeltaLog.forTable(spark, sourceTablePath);
        long sourceVersionAfterUpdate1 = sourceLog.currentSnapshot().snapshot().version();
        LOG.info("Source table version after first update: {}", sourceVersionAfterUpdate1);
        
        // STEP 5: Perform incremental export (from version after initial import to latest)
        String update1ZipPath = tempDir.resolve("update1_delta_export.zip").toString();
        
        DeltaTableExporter update1Exporter = new DeltaTableExporter(
                sourceTablePath, targetVersion + 1, update1ZipPath, exportTempDir.toString(), getS3Settings());
        
        String exportedUpdate1ZipPath = update1Exporter.export();
        LOG.info("Exported incremental update 1 to: {}", exportedUpdate1ZipPath);
        
        // STEP 6: Apply incremental update to target
        DeltaTableImporter update1Importer = new DeltaTableImporter(
                update1ZipPath, targetTablePath, importTempDir.toString(),
                false, true, getS3Settings());
        
        update1Importer.importTable();
        LOG.info("Applied incremental update 1 to target table");
        
        // Verify incremental update was successful
        verifyImportedTable(sourceTablePath, targetTablePath);
        
        // Get target table version after incremental update
        targetLog = DeltaLog.forTable(spark, targetTablePath);
        long targetVersionAfterUpdate1 = targetLog.currentSnapshot().snapshot().version();
        LOG.info("Target table version after incremental update 1: {}", targetVersionAfterUpdate1);
        assertEquals(sourceVersionAfterUpdate1, targetVersionAfterUpdate1,
                "Source and target versions should match after first incremental update");
        
        LOG.info("========== PHASE 3: SECOND UPDATE ==========");
        
        // STEP 7: Perform second round of updates on source table
        LOG.info("Performing second update on source table");
        
        // Add more rows with additional columns
        Dataset<Row> sourceTableWithNewSchema = spark.read().format("delta").load(sourceTablePath)
                .withColumn("updated_at", current_timestamp())
                .withColumn("is_active", lit(true));
        
        sourceTableWithNewSchema.write()
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(sourceTablePath);
        
        // Add more data with the new schema
        Dataset<Row> update2Data = spark.range(2000, 3000)
                .toDF("id")
                .withColumn("value", concat(lit("update2_"), col("id")))
                .withColumn("category", expr("CASE WHEN id % 10 = 0 THEN 'A' WHEN id % 5 = 0 THEN 'B' ELSE 'C' END"))
                .withColumn("created_at", current_timestamp())
                .withColumn("updated_at", current_timestamp())
                .withColumn("is_active", lit(true));
        
        update2Data.write()
                .format("delta")
                .mode("append")
                .save(sourceTablePath);
        
        // Delete some rows
        spark.sql("DELETE FROM delta.`" + sourceTablePath + "` WHERE id % 50 = 0");
        
        // Get source table version after second update
        sourceLog = DeltaLog.forTable(spark, sourceTablePath);
        long sourceVersionAfterUpdate2 = sourceLog.currentSnapshot().snapshot().version();
        LOG.info("Source table version after second update: {}", sourceVersionAfterUpdate2);
        
        // STEP 8: Perform second incremental export
        String update2ZipPath = tempDir.resolve("update2_delta_export.zip").toString();
        
        DeltaTableExporter update2Exporter = new DeltaTableExporter(
                sourceTablePath, targetVersionAfterUpdate1 + 1, update2ZipPath, exportTempDir.toString(), getS3Settings());
        
        String exportedUpdate2ZipPath = update2Exporter.export();
        LOG.info("Exported incremental update 2 to: {}", exportedUpdate2ZipPath);
        
        // STEP 9: Apply second incremental update to target
        DeltaTableImporter update2Importer = new DeltaTableImporter(
                update2ZipPath, targetTablePath, importTempDir.toString(),
                false, true, getS3Settings());
        
        update2Importer.importTable();
        LOG.info("Applied incremental update 2 to target table");
        
        // STEP 10: Final verification
        LOG.info("Performing final verification");
        verifyImportedTable(sourceTablePath, targetTablePath);
        
        // Get final target table version
        targetLog = DeltaLog.forTable(spark, targetTablePath);
        long finalTargetVersion = targetLog.currentSnapshot().snapshot().version();
        LOG.info("Final target table version: {}", finalTargetVersion);
        assertEquals(sourceVersionAfterUpdate2, finalTargetVersion,
                "Source and target versions should match after second incremental update");
        
        // Verify row count and schema
        Dataset<Row> finalSourceTable = spark.read().format("delta").load(sourceTablePath);
        Dataset<Row> finalTargetTable = spark.read().format("delta").load(targetTablePath);
        LOG.info("Final source table has {} rows and {} columns", finalSourceTable.count(), finalSourceTable.columns().length);
        LOG.info("Final target table has {} rows and {} columns", finalTargetTable.count(), finalTargetTable.columns().length);
    }
}
