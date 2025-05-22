package app;

import app.common.storage.S3Settings;
import app.importer.DeltaTableImporter;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * Main entry point for the Delta Table Importer application.
 * This application imports Delta Lake tables from a 7-Zip archive to a target location.
 */
public class ImporterMain {
    private static final Logger LOG = LoggerFactory.getLogger(ImporterMain.class);

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
                formatter.printHelp("delta-table-importer", options);
                return;
            }

            // Validate required options
            if (!cmd.hasOption("a")) {
                throw new ParseException("Missing required option: archive-file");
            }

            if (!cmd.hasOption("t")) {
                throw new ParseException("Missing required option: target-path");
            }

            // Extract command line parameters
            String archivePath = cmd.getOptionValue("a");
            String targetPath = cmd.getOptionValue("t");
            boolean overwrite = cmd.hasOption("o");
            boolean mergeSchema = cmd.hasOption("m");
            
            // Create temporary directory
            String tempDir = cmd.getOptionValue("tmp", 
                    System.getProperty("java.io.tmpdir") + "/delta-import-" + UUID.randomUUID());
            
            // Extract S3 configuration if needed
            S3Settings s3Settings = new S3Settings(
                cmd.getOptionValue("s3-access-key"),
                cmd.getOptionValue("s3-secret-key"),
                cmd.getOptionValue("s3-endpoint"),
                cmd.hasOption("s3-path-style-access")
            );
            
            // Create and run the importer
            DeltaTableImporter importer = new DeltaTableImporter(archivePath, targetPath, tempDir, overwrite, mergeSchema, s3Settings);
            
            LOG.info("Starting Delta Table import process");
            importer.importTable();
            
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
            
            LOG.info("Delta Table import completed successfully");
            
        } catch (ParseException e) {
            LOG.error("Error parsing command line arguments: {}", e.getMessage());
            formatter.printHelp("delta-table-importer", options);
            System.exit(1);
        } catch (Exception e) {
            LOG.error("Error during Delta Table import", e);
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
        options.addOption(Option.builder("a")
                .longOpt("archive-file")
                .desc("Path to the 7-Zip archive containing the Delta table export")
                .hasArg()
                .required()
                .build());
        
        options.addOption(Option.builder("t")
                .longOpt("target-path")
                .desc("Path where the Delta table will be created (s3a://bucket/path/to/table or file:///path/to/table)")
                .hasArg()
                .required()
                .build());
        
        // Import options
        options.addOption(Option.builder("o")
                .longOpt("overwrite")
                .desc("Overwrite the target table if it exists")
                .build());
        
        options.addOption(Option.builder("m")
                .longOpt("merge-schema")
                .desc("Merge the schema with the existing table if it exists")
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
                .desc("Temporary directory to use for extracting files")
                .hasArg()
                .build());
        
        options.addOption(Option.builder("c")
                .longOpt("cleanup")
                .desc("Clean up temporary directory after import")
                .build());
        
        options.addOption(Option.builder("h")
                .longOpt("help")
                .desc("Display help information")
                .build());
        
        return options;
    }
}
