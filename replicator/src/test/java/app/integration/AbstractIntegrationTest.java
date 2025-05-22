package app.integration;

import app.common.storage.S3Settings;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Base class for Delta table export/import integration tests.
 */
public abstract class AbstractIntegrationTest {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractIntegrationTest.class);

    protected SparkSession spark;
    
    @TempDir
    protected Path tempDir;

    protected String sourceTablePath;
    protected String targetTablePath;

    /**
     * Counts files in the given directory by their extension.
     *
     * @param files The list of files to count
     * @return A map of extension to count
     */
    protected Map<String, Long> countFileTypesByExtension(List<String> files) throws IOException {
        return files.stream()
                .collect(Collectors.groupingBy(
                        file -> {
                            if (file.endsWith(".checkpoint.parquet")) {
                                return ".checkpoint.parquet";
                            } else if (file.endsWith(".json")) {
                                return ".json";
                            } else {
                                int lastDotIndex = file.lastIndexOf('.');
                                return lastDotIndex == -1 ? "(no extension)" : file.substring(lastDotIndex);
                            }
                        },
                        Collectors.counting()
                ));
    }
    protected Path zipFilesPath;
    protected Path exportTempDir;
    protected Path importTempDir;

    @BeforeEach
    public void setupLocalPaths() throws IOException {
        importTempDir = tempDir.resolve("import");
        Files.createDirectories(importTempDir);
        exportTempDir = tempDir.resolve("export");
        Files.createDirectories(exportTempDir);
        zipFilesPath = tempDir.resolve("zip");
        Files.createDirectories(zipFilesPath);
    }

    @BeforeEach
    public void createSourceAndTargetPaths() throws IOException {
        Path sourceTablePath = tempDir.resolve("source-table");
        Files.createDirectories(sourceTablePath);
        this.sourceTablePath = sourceTablePath.toString();

        Path targetTablePath = tempDir.resolve("target-table");
        Files.createDirectories(targetTablePath);
        this.targetTablePath = targetTablePath.toString();
    }

    /**
     * Sets up the test environment, including SparkSession initialization.
     */
    @BeforeEach
    public void setupSparkSession() {
        // Initialize Spark with default configuration
        SparkSession.Builder builder = SparkSession.builder()
                .appName(this.getClass().getSimpleName())
                .master("local[*]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog");
        // Let subclasses customize the Spark session
        builder = customizeSparkSession(builder);
        // Create session
        spark = builder.getOrCreate();
    }

    protected SparkSession.Builder customizeSparkSession(SparkSession.Builder builder) {
        // NO-OP by default
        return builder;
    };

    protected S3Settings getS3Settings() {
        // NO-OP by default
        return S3Settings.defaultSettings();
    }

    /**
     * Tears down the test environment and cleans up resources.
     */
    @AfterEach
    public void closeSparkSession() {
        if (spark != null) {
            spark.close();
        }
    }

    public void resetSparkSession() {
        closeSparkSession();
        setupSparkSession();
    }

    /**
     * Verifies that the imported table matches the source table.
     *
     * @param sourcePath Path to the source Delta table
     * @param targetPath Path to the imported Delta table
     */
    protected void verifyImportedTable(String sourcePath, String targetPath) {
        // Reset Spark session as the underlying table have been updated by the test
        resetSparkSession();
        
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
