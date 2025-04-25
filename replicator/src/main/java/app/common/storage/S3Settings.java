package app.common.storage;

/**
 * Record containing all S3-related settings required for connecting to S3 or S3-compatible storage.
 * This record encapsulates all configuration parameters needed to create an S3 client.
 * 
 * @param accessKey The AWS access key
 * @param secretKey The AWS secret key
 * @param endpoint The S3 endpoint URL (for S3-compatible storage)
 * @param pathStyleAccessEnabled Whether to use path-style access (for S3-compatible storage)
 */
public record S3Settings(
    String accessKey,
    String secretKey,
    String endpoint,
    boolean pathStyleAccessEnabled
) {
    /**
     * Creates default S3Settings with all fields set to null or false.
     * 
     * @return Default S3Settings
     */
    public static S3Settings defaultSettings() {
        return new S3Settings(null, null, null, false);
    }
    
    /**
     * Returns true if this instance has valid credentials.
     * 
     * @return true if both accessKey and secretKey are non-null and non-empty
     */
    public boolean hasCredentials() {
        return accessKey != null && !accessKey.isEmpty() && 
               secretKey != null && !secretKey.isEmpty();
    }
    
    /**
     * Returns true if this instance has a custom endpoint configured.
     * 
     * @return true if endpoint is non-null and non-empty
     */
    public boolean hasCustomEndpoint() {
        return endpoint != null && !endpoint.isEmpty();
    }
}
