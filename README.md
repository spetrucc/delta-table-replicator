# Delta Table Replicator

A Java application that allows you to work with Delta Lake tables across environments. The tool provides two main functions:
1. **Export**: Export Delta Lake tables from S3 or local filesystem to a local ZIP archive
2. **Import**: Import previously exported Delta tables to S3 or local filesystem

This tool allows you to create a portable snapshot of a Delta table between two versions and replicate it elsewhere.

## What's Included in the ZIP File

1. Delta log JSON files (`_delta_log/*.json`) from the specified version range
2. The `_last_checkpoint` file (if it exists)
3. Parquet data files referenced in the add operations within the logs
4. Deletion vector files (if any) defined under `add.deletionVector.dvFile`

## Prerequisites

- Java 17 or higher
- Maven 3.6 or higher
- AWS credentials with access to the S3 bucket containing the Delta table (only needed when accessing S3)

## Building

```bash
mvn clean package
```

This will create a JAR file with all dependencies in the `replicator/target` directory.

## Usage

### Export a Delta Table

```bash
java -jar replicator/target/replicator-1.0-SNAPSHOT-jar-with-dependencies.jar \
  --table-path <path-to-delta-table> \
  --output-zip /path/to/output.zip \
  [--from-version 5] \
  [--s3-access-key YOUR_ACCESS_KEY] \
  [--s3-secret-key YOUR_SECRET_KEY] \
  [--s3-endpoint custom-endpoint.example.com] \
  [--s3-path-style-access] \
  [--temp-dir /path/to/temp] \
  [--cleanup]
```

#### Export Command Line Options

| Option | Description |
|--------|-------------|
| `-s, --table-path` | Path to the Delta table (s3a://bucket/path/to/table or file:///path/to/table) |
| `-o, --output-zip` | Local path where the ZIP file will be created |
| `-f, --from-version` | Starting version to export (inclusive, default: 0) |
| `--s3-access-key` | AWS access key (only needed for S3 paths) |
| `--s3-secret-key` | AWS secret key (only needed for S3 paths) |
| `--s3-endpoint` | S3 endpoint (for S3-compatible storage, only needed for S3 paths) |
| `--s3-path-style-access` | Use path-style access (for S3-compatible storage, only needed for S3 paths) |
| `--tmp, --temp-dir` | Temporary directory to use for downloading files |
| `-c, --cleanup` | Clean up temporary directory after export |
| `-h, --help` | Display help information |

### Import a Delta Table

```bash
java -jar replicator/target/replicator-1.0-SNAPSHOT-jar-with-dependencies.jar \
  --archive-file /path/to/delta-export.zip \
  --target-path <path-to-target-location> \
  [--overwrite] \
  [--merge-schema] \
  [--s3-access-key YOUR_ACCESS_KEY] \
  [--s3-secret-key YOUR_SECRET_KEY] \
  [--s3-endpoint custom-endpoint.example.com] \
  [--s3-path-style-access] \
  [--temp-dir /path/to/temp] \
  [--cleanup]
```

#### Import Command Line Options

| Option | Description |
|--------|-------------|
| `-a, --archive-file` | Path to the 7-Zip archive containing the Delta table export |
| `-t, --target-path` | Path where the Delta table will be created (s3a://bucket/path/to/table or file:///path/to/table) |
| `-o, --overwrite` | Overwrite the target table if it exists |
| `-m, --merge-schema` | Merge the schema with the existing table if it exists |
| `--s3-access-key` | AWS access key (only needed for S3 paths) |
| `--s3-secret-key` | AWS secret key (only needed for S3 paths) |
| `--s3-endpoint` | S3 endpoint (for S3-compatible storage, only needed for S3 paths) |
| `--s3-path-style-access` | Use path-style access (for S3-compatible storage, only needed for S3 paths) |
| `--tmp, --temp-dir` | Temporary directory to use for extracting files |
| `-c, --cleanup` | Clean up temporary directory after import |
| `-h, --help` | Display help information |

## AWS Credentials

You can provide AWS credentials in several ways (only needed when accessing S3):
1. Using the `--s3-access-key` and `--s3-secret-key` command line options
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
  --from-version 10
```

### Using a custom S3-compatible endpoint
```bash
java -jar replicator/target/replicator-1.0-SNAPSHOT-jar-with-dependencies.jar \
  --table-path s3a://my-bucket/my-delta-table \
  --output-zip ~/exports/my-table-export.zip \
  --s3-endpoint https://minio.example.com \
  --s3-path-style-access
```

### Import a Delta table to S3
```bash
java -jar replicator/target/replicator-1.0-SNAPSHOT-jar-with-dependencies.jar \
  --archive-file ~/exports/my-table-export.zip \
  --target-path s3a://my-bucket/my-imported-delta-table
```

### Import a Delta table to local filesystem with overwrite
```bash
java -jar replicator/target/replicator-1.0-SNAPSHOT-jar-with-dependencies.jar \
  --archive-file ~/exports/my-table-export.zip \
  --target-path file:///path/to/target-delta-table \
  --overwrite
```

## How It Works

### Export Process
1. The application detects whether the Delta table is on S3 or local filesystem based on the table path
2. It connects to the appropriate filesystem using the Hadoop FileSystem API
3. It reads Delta log files directly from the source for the specified version range and up to the latest version available
4. Each log entry is parsed to extract data file paths and deletion vector paths
5. All required files are downloaded to a temporary local folder
6. A ZIP archive is created with the correct internal structure
7. The temporary directory is cleaned up if the `--cleanup` option is specified

### Import Process
1. The ZIP archive is extracted to a temporary directory
2. The application detects whether the target location is on S3 or local filesystem based on the path
3. It connects to the appropriate filesystem using the Hadoop FileSystem API
4. All files from the extracted archive are uploaded to the target location, maintaining the Delta table structure
5. If overwrite is specified, any existing table at the location will be overwritten
6. If merge-schema is specified, the schema will be merged with any existing table at the location
7. The temporary directory is cleaned up if the `--cleanup` option is specified

## License

This project is licensed under the MIT License - see the LICENSE file for details.
