package app;

import app.exporter.DeltaTableExporter;
import app.common.storage.StorageProvider;
import app.common.storage.StorageProviderFactory;
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
            
            // Parse max ZIP size if provided
            long maxZipSize = 2L * 1024 * 1024 * 1024; // Default 2GB
            if (cmd.hasOption("m")) {
                String maxZipSizeStr = cmd.getOptionValue("m");
                maxZipSize = parseSize(maxZipSizeStr);
                LOG.info("Using maximum ZIP volume size: {} bytes", maxZipSize);
            }
            
            // Create temporary directory
            String tempDir = cmd.getOptionValue("tmp", 
                    System.getProperty("java.io.tmpdir") + "/delta-export-" + UUID.randomUUID());
            
            // Extract S3 configuration if needed
            String accessKey = cmd.getOptionValue("ak");
            String secretKey = cmd.getOptionValue("sk");
            String endpoint = cmd.getOptionValue("e");
            boolean pathStyleAccess = cmd.hasOption("psa");
            
            // Create storage provider based on the table path
            StorageProvider storageProvider;
            if (tablePath.startsWith("s3://") || tablePath.startsWith("s3a://")) {
                LOG.info("Using S3 storage provider with provided credentials");
                storageProvider = StorageProviderFactory.createProvider(
                        tablePath, accessKey, secretKey, endpoint, pathStyleAccess);
            } else {
                LOG.info("Using local storage provider");
                storageProvider = StorageProviderFactory.createProvider(tablePath);
            }
            
            // Create and run the exporter
            DeltaTableExporter exporter = new DeltaTableExporter(
                    tablePath, fromVersion, outputZipPath, tempDir, storageProvider, maxZipSize);
            
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
        
        // ZIP file options
        options.addOption(Option.builder("m")
                .longOpt("max-zip-size")
                .desc("Maximum size of each ZIP volume in bytes (default: 2GB). " +
                      "Use suffixes K, M, or G for kilobytes, megabytes, or gigabytes.")
                .hasArg()
                .build());
        
        // S3 configuration options
        options.addOption(Option.builder("ak")
                .longOpt("access-key")
                .desc("AWS access key (only needed for S3 paths)")
                .hasArg()
                .build());
        
        options.addOption(Option.builder("sk")
                .longOpt("secret-key")
                .desc("AWS secret key (only needed for S3 paths)")
                .hasArg()
                .build());
        
        options.addOption(Option.builder("e")
                .longOpt("endpoint")
                .desc("S3 endpoint (for S3-compatible storage, only needed for S3 paths)")
                .hasArg()
                .build());
        
        options.addOption(Option.builder("psa")
                .longOpt("path-style-access")
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

    /**
     * Parses a size string like "1G" or "500M" into bytes.
     *
     * @param sizeStr The size string to parse (e.g. "1G", "500M", "1024K", "1048576")
     * @return The size in bytes
     */
    private static long parseSize(String sizeStr) {
        sizeStr = sizeStr.trim().toUpperCase();
        long multiplier = 1;
        
        if (sizeStr.endsWith("K")) {
            multiplier = 1024L;
            sizeStr = sizeStr.substring(0, sizeStr.length() - 1);
        } else if (sizeStr.endsWith("M")) {
            multiplier = 1024L * 1024L;
            sizeStr = sizeStr.substring(0, sizeStr.length() - 1);
        } else if (sizeStr.endsWith("G")) {
            multiplier = 1024L * 1024L * 1024L;
            sizeStr = sizeStr.substring(0, sizeStr.length() - 1);
        }
        
        try {
            return Long.parseLong(sizeStr) * multiplier;
        } catch (NumberFormatException e) {
            LOG.warn("Invalid size format: {}. Using default size.", sizeStr);
            return 2L * 1024L * 1024L * 1024L; // Default to 2GB
        }
    }
}