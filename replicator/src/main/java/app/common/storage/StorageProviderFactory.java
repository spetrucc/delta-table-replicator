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
        if (basePath == null || basePath.isEmpty()) {
            throw new IllegalArgumentException("Base path cannot be null or empty");
        }
        
        if (basePath.startsWith("s3://") || basePath.startsWith("s3a://")) {
            LOG.info("Creating S3 storage provider for path: {}", basePath);
            return new S3StorageProvider(basePath);
        } else {
            LOG.info("Creating local storage provider for path: {}", basePath);
            return new LocalStorageProvider(basePath);
        }
    }
    
    /**
     * Creates a storage provider for the given path with customized S3 connection parameters.
     *
     * @param basePath The base path for the storage provider
     * @param accessKey The AWS access key (for S3 paths)
     * @param secretKey The AWS secret key (for S3 paths)
     * @param endpoint The S3 endpoint (for S3-compatible storage)
     * @param pathStyleAccess Whether to use path-style access (for S3-compatible storage)
     * @return A storage provider for the given path
     * @throws IOException If an I/O error occurs
     */
    public static StorageProvider createProvider(String basePath, String accessKey, String secretKey, 
                                               String endpoint, boolean pathStyleAccess) throws IOException {
        if (basePath == null || basePath.isEmpty()) {
            throw new IllegalArgumentException("Base path cannot be null or empty");
        }
        
        if (basePath.startsWith("s3://") || basePath.startsWith("s3a://")) {
            LOG.info("Creating S3 storage provider for path: {}", basePath);
            return new S3StorageProvider(basePath, accessKey, secretKey, endpoint, pathStyleAccess);
        } else {
            LOG.info("Creating local storage provider for path: {}", basePath);
            return new LocalStorageProvider(basePath);
        }
    }
}
