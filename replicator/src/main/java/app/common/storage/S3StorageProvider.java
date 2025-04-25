package app.common.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Storage provider implementation for AWS S3.
 */
public class S3StorageProvider implements StorageProvider {
    private static final Logger LOG = LoggerFactory.getLogger(S3StorageProvider.class);
    
    private final String basePath;
    private final String bucket;
    private final String prefix;
    private final S3Client s3Client;

    /**
     * Creates a new S3StorageProvider.
     *
     * @param basePath The S3 path in the format s3a://bucket/prefix or s3://bucket/prefix
     * @param accessKey The AWS access key
     * @param secretKey The AWS secret key
     * @param endpoint The S3 endpoint (optional, for S3-compatible storage)
     * @param pathStyleAccess Whether to use path-style access (for S3-compatible storage)
     * @deprecated Use {@link #S3StorageProvider(String, S3Settings)} instead
     */
    @Deprecated
    public S3StorageProvider(String basePath, String accessKey, String secretKey, 
                            String endpoint, boolean pathStyleAccess) {
        this(basePath, new S3Settings(accessKey, secretKey, endpoint, pathStyleAccess));
    }
    
    /**
     * Creates a new S3StorageProvider.
     *
     * @param basePath The S3 path in the format s3a://bucket/prefix or s3://bucket/prefix
     * @param s3Settings The S3 connection settings
     */
    public S3StorageProvider(String basePath, S3Settings s3Settings) {
        this.basePath = basePath;
        
        // Parse bucket and prefix from the basePath
        String path = basePath.replaceFirst("^s3a://|^s3://", "");
        int slashIndex = path.indexOf('/');
        if (slashIndex > 0) {
            this.bucket = path.substring(0, slashIndex);
            this.prefix = path.substring(slashIndex + 1);
        } else {
            this.bucket = path;
            this.prefix = "";
        }
        
        // Build S3 client
        S3ClientBuilder builder = S3Client.builder()
                .region(Region.US_EAST_1); // Default region, can be overridden
        
        // Configure credentials if provided
        if (s3Settings.hasCredentials()) {
            AwsBasicCredentials credentials = AwsBasicCredentials.create(
                    s3Settings.accessKey(), s3Settings.secretKey());
            builder.credentialsProvider(StaticCredentialsProvider.create(credentials));
        }
        
        // Configure endpoint if provided (for S3-compatible storage)
        if (s3Settings.hasCustomEndpoint()) {
            builder.endpointOverride(URI.create(s3Settings.endpoint()));
        }
        
        // Configure path-style access if requested
        if (s3Settings.pathStyleAccessEnabled()) {
            builder.serviceConfiguration(s3 -> s3.pathStyleAccessEnabled(true));
        }
        
        this.s3Client = builder.build();
        LOG.info("Initialized S3 storage provider with bucket: {}, prefix: {}", bucket, prefix);
    }
    
    @Override
    public boolean fileExists(String path) throws IOException {
        String key = getS3Key(path);
        try {
            HeadObjectRequest request = HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
            s3Client.headObject(request);
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        } catch (S3Exception e) {
            throw new IOException("Error checking if file exists in S3: " + e.getMessage(), e);
        }
    }
    
    @Override
    public boolean directoryExists(String path) throws IOException {
        String key = getS3Key(path);
        if (!key.endsWith("/")) {
            key += "/";
        }
        
        try {
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(key)
                    .maxKeys(1)
                    .build();
            ListObjectsV2Response response = s3Client.listObjectsV2(request);
            return response.hasContents();
        } catch (S3Exception e) {
            throw new IOException("Error checking if directory exists in S3: " + e.getMessage(), e);
        }
    }
    
    @Override
    public void createDirectory(String path) throws IOException {
        String key = getS3Key(path);
        if (!key.endsWith("/")) {
            key += "/";
        }
        
        try {
            // S3 doesn't have real directories, just create an empty object with a trailing slash
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
            s3Client.putObject(request, RequestBody.fromBytes(new byte[0]));
        } catch (S3Exception e) {
            throw new IOException("Error creating directory in S3: " + e.getMessage(), e);
        }
    }
    
    @Override
    public List<String> listFiles(String directoryPath, String pattern) throws IOException {
        String key = getS3Key(directoryPath);
        if (!key.endsWith("/")) {
            key += "/";
        }
        
        try {
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(key)
                    .build();
            
            List<String> matchingFiles = new ArrayList<>();
            ListObjectsV2Response response;
            do {
                response = s3Client.listObjectsV2(request);

                String finalKey = key;
                List<String> matches = response.contents().stream()
                        .map(S3Object::key)
                        .filter(k -> !k.endsWith("/")) // Skip directories
                        .map(k -> k.substring(finalKey.length())) // Get relative path
                        .filter(name -> name.matches(pattern))
                        .map(name -> directoryPath + "/" + name)
                        .toList();
                
                matchingFiles.addAll(matches);
                
                // Continue if there are more results
                String token = response.nextContinuationToken();
                if (token == null) {
                    break;
                }
                
                request = request.toBuilder()
                        .continuationToken(token)
                        .build();
                
            } while (response.isTruncated());
            
            return matchingFiles;
        } catch (S3Exception e) {
            throw new IOException("Error listing files in S3: " + e.getMessage(), e);
        }
    }
    
    @Override
    public InputStream getInputStream(String path) throws IOException {
        String key = getS3Key(path);
        try {
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();

            return s3Client.getObject(request);
        } catch (S3Exception e) {
            throw new IOException("Error getting input stream from S3: " + e.getMessage(), e);
        }
    }
    
    @Override
    public OutputStream getOutputStream(String path) throws IOException {
        // S3 doesn't support direct output streams, so we use a temporary file
        // and upload it when closed
        return new S3OutputStream(path);
    }
    
    @Override
    public void uploadFile(String sourcePath, String targetPath) throws IOException {
        String key = getS3Key(targetPath);
        
        try {
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
            
            s3Client.putObject(request, RequestBody.fromFile(Paths.get(sourcePath)));
        } catch (S3Exception e) {
            throw new IOException("Error uploading file to S3: " + e.getMessage(), e);
        }
    }
    
    @Override
    public void downloadFile(String sourcePath, String targetPath) throws IOException {
        String key = getS3Key(sourcePath);
        Path target = Paths.get(targetPath);
        
        try {
            // Create parent directories if they don't exist
            Files.createDirectories(target.getParent());
            
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
            
            s3Client.getObject(request, ResponseTransformer.toFile(target));
        } catch (S3Exception e) {
            throw new IOException("Error downloading file from S3: " + e.getMessage(), e);
        }
    }
    
    @Override
    public String readFileAsString(String path) throws IOException {
        try (InputStream is = getInputStream(path)) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
    
    @Override
    public void writeStringToFile(String path, String content) throws IOException {
        String key = getS3Key(path);
        
        try {
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
            
            s3Client.putObject(request, RequestBody.fromString(content, StandardCharsets.UTF_8));
        } catch (S3Exception e) {
            throw new IOException("Error writing string to S3: " + e.getMessage(), e);
        }
    }
    
    @Override
    public String getBasePath() {
        return basePath;
    }
    
    @Override
    public String resolvePath(String relativePath) {
        // Handle absolute paths within the bucket
        if (relativePath.startsWith("/")) {
            return "s3a://" + bucket + relativePath;
        }
        
        // Handle relative paths
        String resolvedPrefix = prefix.isEmpty() ? "" : prefix + "/";
        return "s3a://" + bucket + "/" + resolvedPrefix + relativePath;
    }
    
    /**
     * Converts a path to an S3 key.
     *
     * @param path The path to convert
     * @return The S3 key
     */
    private String getS3Key(String path) {
        // Remove s3a:// or s3:// prefix if present
        String normalizedPath = path.replaceFirst("^s3a://[^/]+/|^s3://[^/]+/", "");
        
        // Handle absolute paths
        if (normalizedPath.startsWith("/")) {
            return normalizedPath.substring(1);
        }
        
        // Handle relative paths
        if (prefix.isEmpty()) {
            return normalizedPath;
        } else {
            return prefix + "/" + normalizedPath;
        }
    }
    
    @Override
    public void close() {
        if (s3Client != null) {
            s3Client.close();
        }
    }
    
    /**
     * Output stream implementation for S3.
     * Uses a temporary file and uploads it to S3 when closed.
     */
    private class S3OutputStream extends OutputStream {
        private final String path;
        private final Path tempFile;
        private final OutputStream tempOut;
        
        public S3OutputStream(String path) throws IOException {
            this.path = path;
            this.tempFile = Files.createTempFile("s3upload", ".tmp");
            this.tempOut = Files.newOutputStream(tempFile);
        }
        
        @Override
        public void write(int b) throws IOException {
            tempOut.write(b);
        }
        
        @Override
        public void write(byte[] b) throws IOException {
            tempOut.write(b);
        }
        
        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            tempOut.write(b, off, len);
        }
        
        @Override
        public void flush() throws IOException {
            tempOut.flush();
        }
        
        @Override
        public void close() throws IOException {
            tempOut.close();
            
            try {
                // Upload the temp file to S3
                String key = getS3Key(path);
                
                PutObjectRequest request = PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .build();
                
                s3Client.putObject(request, RequestBody.fromFile(tempFile));
            } catch (S3Exception e) {
                throw new IOException("Error uploading file to S3: " + e.getMessage(), e);
            } finally {
                // Delete the temp file
                Files.deleteIfExists(tempFile);
            }
        }
    }
}
