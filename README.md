# Delta Table Replicator

A Java application that exports Delta Lake tables from S3 or local filesystem to a local ZIP archive. This tool allows you to create a portable snapshot of a Delta table between two versions, including all necessary files to replicate the table elsewhere.

## What's Included in the ZIP File

1. Delta log JSON files (`_delta_log/*.json`) from the specified version range
2. The `_last_checkpoint` file (if it exists)
3. Parquet data files referenced in the add operations within the logs
4. Deletion vector files (if any) defined under `add.deletionVector.dvFile`

## Prerequisites

- Java 21 or higher
- Maven 3.6 or higher
- AWS credentials with access to the S3 bucket containing the Delta table (only needed when accessing S3)

## Building

```bash
mvn clean package
```

This will create a JAR file with all dependencies in the `replicator/target` directory.

## Usage

```bash
java -jar replicator/target/replicator-1.0-SNAPSHOT-jar-with-dependencies.jar \
  --table-path <path-to-delta-table> \
  --output-zip /path/to/output.zip \
  [--from-version 5] \
  [--to-version 10] \
  [--access-key YOUR_ACCESS_KEY] \
  [--secret-key YOUR_SECRET_KEY] \
  [--endpoint custom-endpoint.example.com] \
  [--path-style-access] \
  [--temp-dir /path/to/temp] \
  [--cleanup]
```

### Command Line Options

| Option | Description |
|--------|-------------|
| `-s, --table-path` | Path to the Delta table (s3a://bucket/path/to/table or file:///path/to/table) |
| `-o, --output-zip` | Local path where the ZIP file will be created |
| `-f, --from-version` | Starting version to export (inclusive, default: 0) |
| `-t, --to-version` | Ending version to export (inclusive, default: latest) |
| `-ak, --access-key` | AWS access key (only needed for S3 paths) |
| `-sk, --secret-key` | AWS secret key (only needed for S3 paths) |
| `-e, --endpoint` | S3 endpoint (for S3-compatible storage, only needed for S3 paths) |
| `-psa, --path-style-access` | Use path-style access (for S3-compatible storage, only needed for S3 paths) |
| `-tmp, --temp-dir` | Temporary directory to use for downloading files |
| `-c, --cleanup` | Clean up temporary directory after export |
| `-h, --help` | Display help information |

## AWS Credentials

You can provide AWS credentials in several ways (only needed when accessing S3):
1. Using the `--access-key` and `--secret-key` command line options
2. Using environment variables (`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`)
3. Using AWS credentials file (`~/.aws/credentials`)
4. Using EC2 instance profiles or container roles if running on AWS

## Examples

### Export an entire Delta table from S3
```bash
java -jar replicator/target/replicator-1.0-SNAPSHOT-jar-with-dependencies.jar \
  --table-path s3a://my-bucket/my-delta-table \
  --output-zip ~/exports/my-table-export.zip
```

### Export a Delta table from local filesystem
```bash
java -jar replicator/target/replicator-1.0-SNAPSHOT-jar-with-dependencies.jar \
  --table-path file:///path/to/my-delta-table \
  --output-zip ~/exports/my-table-export.zip
```

### Export a specific version range
```bash
java -jar replicator/target/replicator-1.0-SNAPSHOT-jar-with-dependencies.jar \
  --table-path s3a://my-bucket/my-delta-table \
  --output-zip ~/exports/my-table-export.zip \
  --from-version 10 \
  --to-version 20
```

### Using a custom S3-compatible endpoint
```bash
java -jar replicator/target/replicator-1.0-SNAPSHOT-jar-with-dependencies.jar \
  --table-path s3a://my-bucket/my-delta-table \
  --output-zip ~/exports/my-table-export.zip \
  --endpoint https://minio.example.com \
  --path-style-access
```

## How It Works

1. The application detects whether the Delta table is on S3 or local filesystem based on the table path
2. It connects to the appropriate filesystem using the Hadoop FileSystem API
3. It reads Delta log files directly from the source for the specified version range
4. Each log entry is parsed to extract data file paths and deletion vector paths
5. All required files are downloaded to a temporary local folder
6. A ZIP archive is created with the correct internal structure
7. The temporary directory is cleaned up if the `--cleanup` option is specified

## License

This project is licensed under the MIT License - see the LICENSE file for details.
