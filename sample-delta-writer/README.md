# Sample Delta Table Application

This sample application demonstrates how to create a Delta table and merge records from a CSV file using Delta Lake and Apache Spark.

## Features

- Creates a Delta table if it doesn't exist
- Reads data from a CSV file with 100 records
- Generates new random values for each record on every run to ensure updates
- Merges records into the Delta table (updates existing records, inserts new ones)
- Displays the data before and after the merge operation

## Prerequisites

- Java 21 or higher
- Maven 3.6 or higher

## Building

```bash
mvn clean package
```

This will create a JAR file with all dependencies in the `target` directory.

## Usage

```bash
java -jar target/sample-delta-app-1.0-SNAPSHOT-jar-with-dependencies.jar \
  [--delta-table-path PATH] \
  [--csv-file PATH]
```

### Command Line Options

| Option | Description |
|--------|-------------|
| `-d, --delta-table-path` | Path to the Delta table (default: delta-table) |
| `-c, --csv-file` | Path to the CSV file (default: src/main/resources/sample_data_100.csv) |
| `-h, --help` | Display help information |

## Examples

### Run with default settings

```bash
java -jar target/sample-delta-app-1.0-SNAPSHOT-jar-with-dependencies.jar
```

This will:
1. Create a Delta table at `./delta-table` if it doesn't exist
2. Read data from `src/main/resources/sample_data_100.csv`
3. Generate new random values for each record
4. Merge the data into the Delta table

### Demonstrate update functionality with repeated runs

```bash
# First run to create or update the Delta table
java -jar target/sample-delta-app-1.0-SNAPSHOT-jar-with-dependencies.jar

# Run again to update the random_value column for all records
java -jar target/sample-delta-app-1.0-SNAPSHOT-jar-with-dependencies.jar
```

Each time you run the application, it will:
1. Read the same 100 records from the CSV file
2. Generate new random values for each record
3. Update all records in the Delta table with the new random values

## How It Works

The application:

1. Creates a Spark session configured for Delta Lake
2. Checks if the Delta table exists at the specified path
3. If the table doesn't exist, creates it with the appropriate schema
4. Reads data from the specified CSV file (100 records)
5. Generates new random values for each record to ensure updates happen on each run
6. Performs a merge operation on the Delta table:
   - If a record with the same ID exists, it updates all fields (including the new random value)
   - If a record with the ID doesn't exist, it inserts a new record
7. Displays a sample of the data in the Delta table after the merge

## Schema

The Delta table and CSV files use the following schema:

| Column | Type | Description |
|--------|------|-------------|
| id | Integer | Primary key used for matching records during merge |
| name | String | Person's name |
| age | Integer | Person's age |
| city | String | Person's city |
| timestamp | Timestamp | Timestamp of the record |
| random_value | Double | Random value that changes on each run to demonstrate updates |
