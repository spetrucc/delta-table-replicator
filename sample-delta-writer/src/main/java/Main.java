import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static final Random RANDOM = new Random();

    public static void main(String[] args) throws IOException {

        // Get parameters
        String deltaTablePath = "/tmp/source_table";
        String csvFilePath = "sample-delta-writer/src/main/resources/sample_data_100.csv";

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[4]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
                .config("spark.databricks.delta.properties.defaults.enableDeletionVectors", "true")
                .getOrCreate();

        spark.log().info("*** SparkSession created ***");

//        // Define schema for our data
//        StructType schema = new StructType()
//                .add("id", DataTypes.IntegerType)
//                .add("name", DataTypes.StringType)
//                .add("age", DataTypes.IntegerType)
//                .add("city", DataTypes.StringType)
//                .add("timestamp", DataTypes.TimestampType)
//                .add("random_value", DataTypes.DoubleType);
//
//        DeltaTable deltaTable = DeltaTable.createIfNotExists(spark)
//                .location(deltaTablePath)
//                .addColumns(schema)
//                .execute();
//
//        deltaTable.addFeatureSupport("timestampNtz");
//        deltaTable.addFeatureSupport("deletionVectors");
//
//        spark.log().info("*** DeltaTable opened ***");
//
//        // Read CSV file
//        LOG.info("Reading CSV file: {}", csvFilePath);
//        Dataset<Row> inputData = spark.read()
//                .option("header", "true")
//                .option("inferSchema", "false")
//                .schema(schema)
//                .csv(csvFilePath);
//
//        // Generate new random values for each record to ensure updates happen
//        // Register a UDF to generate random values
//        spark.udf().register("generateRandomValue", () -> RANDOM.nextDouble(), DataTypes.DoubleType);
//
//        // Create a temporary view of the CSV data
//        inputData.createOrReplaceTempView("csv_data");
//
//        // Generate new random values for each record
//        Dataset<Row> dataWithNewRandomValues = spark.sql("SELECT id, name, age, city, timestamp, generateRandomValue() as random_value FROM csv_data");
//
//        spark.log().info("*** Dataset update ready ***");
//
//        deltaTable
//                .as("dst")
//                .merge(dataWithNewRandomValues.as("src").toDF(), "src.id = dst.id")
//                .whenMatched().updateAll()
//                .whenNotMatched().insertAll()
//                .execute();
//
//        spark.log().info("*** MERGE completed ***");


        spark.sql("DELETE FROM delta.`" + deltaTablePath + "` where id=49");

        spark.log().info("*** DELETE completed ***");

        Dataset<Row> query = spark.sql("select * from delta.`" + deltaTablePath + "`");

        query.show();

        

    }
}
