package app.common.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Factory for creating storage providers.
 */
public class StorageProviderFactory {
    private static final Logger LOG = LoggerFactory.getLogger(StorageProviderFactory.class);
    
    /**
     * Creates a storage provider for the given path.
     * The provider type is determined by the path prefix.
     *
     * @param basePath The base path for the storage provider
     * @return A storage provider for the given path
     * @throws IOException If an I/O error occurs
     */
    public static StorageProvider createProvider(String basePath) throws IOException {
        return createProvider(basePath, S3Settings.defaultSettings());
    }
    
    /**
     * Creates a storage provider for the given path with customized S3 connection parameters.
     *
     * @param basePath The base path for the storage provider
     * @param s3Settings The S3 connection settings (only used for S3 paths)
     * @return A storage provider for the given path
     * @throws IOException If an I/O error occurs
     */
    public static StorageProvider createProvider(String basePath, S3Settings s3Settings) throws IOException {
        if (basePath == null || basePath.isEmpty()) {
            throw new IllegalArgumentException("Base path cannot be null or empty");
        }
        
        if (basePath.startsWith("s3://") || basePath.startsWith("s3a://")) {
            LOG.info("Creating S3 storage provider for path: {}", basePath);
            return new S3StorageProvider(basePath, s3Settings);
        } else {
            LOG.info("Creating local storage provider for path: {}", basePath);
            return new LocalStorageProvider(basePath);
        }
    }
}
