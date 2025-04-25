package app;

import app.common.storage.S3Settings;
import app.common.storage.StorageProvider;
import app.common.storage.StorageProviderFactory;
import app.exporter.DeltaTableExporter;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * Main entry point for the Delta Table Exporter application.
 * This application exports Delta Lake tables from S3 or local filesystem to a local ZIP archive.
 */
public class ExporterMain {
    private static final Logger LOG = LoggerFactory.getLogger(ExporterMain.class);

    public static void main(String[] args) {
        // Define command line options
        Options options = createCommandLineOptions();
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
            // Parse command line arguments
            CommandLine cmd = parser.parse(options, args);

            // Display help if requested
            if (cmd.hasOption("h")) {
                formatter.printHelp("delta-table-replicator", options);
                return;
            }

            // Validate required options
            if (!cmd.hasOption("s")) {
                throw new ParseException("Missing required option: table-path");
            }

            if (!cmd.hasOption("o")) {
                throw new ParseException("Missing required option: output-zip");
            }

            // Extract command line parameters
            String tablePath = cmd.getOptionValue("s");
            String outputZipPath = cmd.getOptionValue("o");
            long fromVersion = Long.parseLong(cmd.getOptionValue("f", "0"));
            
            // Create temporary directory
            String tempDir = cmd.getOptionValue("tmp", 
                    System.getProperty("java.io.tmpdir") + "/delta-export-" + UUID.randomUUID());
            
            // Extract S3 configuration if needed
            S3Settings s3Settings = new S3Settings(
                cmd.getOptionValue("s3-access-key"),
                cmd.getOptionValue("s3-secret-key"),
                cmd.getOptionValue("s3-endpoint"),
                cmd.hasOption("s3-path-style-access")
            );
            
            // Create storage provider based on the table path
            StorageProvider storageProvider;
            if (tablePath.startsWith("s3://") || tablePath.startsWith("s3a://")) {
                LOG.info("Using S3 storage provider with provided credentials");
                storageProvider = StorageProviderFactory.createProvider(tablePath, s3Settings);
            } else {
                LOG.info("Using local storage provider");
                storageProvider = StorageProviderFactory.createProvider(tablePath);
            }
            
            // Create and run the exporter
            DeltaTableExporter exporter = new DeltaTableExporter(
                    tablePath, fromVersion, outputZipPath, tempDir, storageProvider);
            
            LOG.info("Starting Delta Table export process");
            String finalOutputPath = exporter.export();
            
            // Clean up temporary directory if requested
            if (cmd.hasOption("c")) {
                LOG.info("Cleaning up temporary directory: {}", tempDir);
                Files.walk(Paths.get(tempDir))
                        .sorted((a, b) -> -a.compareTo(b))
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException e) {
                                LOG.warn("Failed to delete: {}", path, e);
                            }
                        });
            }
            
            LOG.info("Delta Table export completed successfully. Final output path: {}", finalOutputPath);
            
        } catch (ParseException e) {
            LOG.error("Error parsing command line arguments: {}", e.getMessage());
            formatter.printHelp("delta-table-replicator", options);
            System.exit(1);
        } catch (Exception e) {
            LOG.error("Error during Delta Table export", e);
            System.exit(1);
        }
    }

    /**
     * Creates the command line options for the application.
     *
     * @return The Options object with all defined options
     */
    private static Options createCommandLineOptions() {
        Options options = new Options();
        
        // Required options
        options.addOption(Option.builder("s")
                .longOpt("table-path")
                .desc("Path to the Delta table (s3a://bucket/path/to/table or file:///path/to/table)")
                .hasArg()
                .required()
                .build());
        
        options.addOption(Option.builder("o")
                .longOpt("output-zip")
                .desc("Local path where the ZIP file will be created")
                .hasArg()
                .required()
                .build());
        
        // Optional version range
        options.addOption(Option.builder("f")
                .longOpt("from-version")
                .desc("Starting version to export (inclusive, default: 0)")
                .hasArg()
                .build());
        
        // S3 configuration options
        options.addOption(Option.builder("s3-access-key")
                .longOpt("s3-access-key")
                .desc("AWS access key (only needed for S3 paths)")
                .hasArg()
                .build());
        
        options.addOption(Option.builder("s3-secret-key")
                .longOpt("s3-secret-key")
                .desc("AWS secret key (only needed for S3 paths)")
                .hasArg()
                .build());
        
        options.addOption(Option.builder("s3-endpoint")
                .longOpt("s3-endpoint")
                .desc("S3 endpoint (for S3-compatible storage, only needed for S3 paths)")
                .hasArg()
                .build());
        
        options.addOption(Option.builder("s3-path-style-access")
                .longOpt("s3-path-style-access")
                .desc("Use path-style access (for S3-compatible storage, only needed for S3 paths)")
                .build());
        
        // Other options
        options.addOption(Option.builder("tmp")
                .longOpt("temp-dir")
                .desc("Temporary directory to use for downloading files")
                .hasArg()
                .build());
        
        options.addOption(Option.builder("c")
                .longOpt("cleanup")
                .desc("Clean up temporary directory after export")
                .build());
        
        options.addOption(Option.builder("h")
                .longOpt("help")
                .desc("Display help information")
                .build());
        
        return options;
    }
}