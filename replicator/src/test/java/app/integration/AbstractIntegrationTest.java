package app.integration;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Base class for Delta table export/import integration tests.
 */
public abstract class AbstractIntegrationTest {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractIntegrationTest.class);
    protected static SparkSession spark;
    
    @TempDir
    protected static Path tempDir;
    
    /**
     * Sets up the test environment, including SparkSession initialization.
     */
    @BeforeAll
    public static void setup() {
        // Initialize Spark with appropriate configurations
        spark = SparkSession.builder()
                .appName("DeltaTableIntegrationTest")
                .master("local[*]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate();
    }
    
    /**
     * Tears down the test environment and cleans up resources.
     */
    @AfterAll
    public static void tearDown() {
        if (spark != null) {
            spark.close();
        }
    }
    
    /**
     * Verifies that the imported table matches the source table.
     *
     * @param sourcePath Path to the source Delta table
     * @param targetPath Path to the imported Delta table
     */
    protected void verifyImportedTable(String sourcePath, String targetPath) {
        // Reset Spark session as the underlying table have been updated by the test
        tearDown();
        setup();
        
        // Read both tables
        Dataset<Row> sourceDF = spark.read().format("delta").load(sourcePath);
        Dataset<Row> targetDF = spark.read().format("delta").load(targetPath);
        
        // Compare row counts
        long sourceCount = sourceDF.count();
        long targetCount = targetDF.count();
        LOG.info("Source table has {} rows, target table has {} rows", sourceCount, targetCount);
        assertEquals(sourceCount, targetCount, "Row counts should match");
        
        // Compare schemas
        assertEquals(sourceDF.schema().treeString(), targetDF.schema().treeString(), "Schemas should match");
        
        // Compare data checksums (order by id to ensure deterministic comparison)
        long sourceChecksum = sourceDF.orderBy("id").select(sum(hash(col("*")))).first().getLong(0);
        long targetChecksum = targetDF.orderBy("id").select(sum(hash(col("*")))).first().getLong(0);
        LOG.info("Source data checksum: {}, target data checksum: {}", sourceChecksum, targetChecksum);
        assertEquals(sourceChecksum, targetChecksum, "Data checksums should match");
    }
}
