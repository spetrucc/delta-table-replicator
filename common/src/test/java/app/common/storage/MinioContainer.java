package app.common.storage;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * TestContainers implementation for MinIO, an S3-compatible object storage.
 */
public class MinioContainer extends GenericContainer<MinioContainer> {
    private static final int DEFAULT_PORT = 9000;
    private static final String DEFAULT_IMAGE = "minio/minio:RELEASE.2023-09-04T19-57-37Z";
    private static final String DEFAULT_USER = "minioadmin";
    private static final String DEFAULT_PASSWORD = "minioadmin";
    
    private String accessKey = DEFAULT_USER;
    private String secretKey = DEFAULT_PASSWORD;
    
    /**
     * Creates a new MinIO container with the default image.
     */
    public MinioContainer() {
        this(DEFAULT_IMAGE);
    }
    
    /**
     * Creates a new MinIO container with the specified image.
     *
     * @param dockerImageName the docker image name
     */
    public MinioContainer(String dockerImageName) {
        super(DockerImageName.parse(dockerImageName));
        withCommand("server /data");
        withExposedPorts(DEFAULT_PORT);
        withEnv("MINIO_ROOT_USER", accessKey);
        withEnv("MINIO_ROOT_PASSWORD", secretKey);
    }
    
    /**
     * Sets the MinIO access key.
     *
     * @param accessKey the access key
     * @return this container instance
     */
    public MinioContainer withUserName(String accessKey) {
        this.accessKey = accessKey;
        withEnv("MINIO_ROOT_USER", accessKey);
        return this;
    }
    
    /**
     * Sets the MinIO secret key.
     *
     * @param secretKey the secret key
     * @return this container instance
     */
    public MinioContainer withPassword(String secretKey) {
        this.secretKey = secretKey;
        withEnv("MINIO_ROOT_PASSWORD", secretKey);
        return this;
    }
    
    /**
     * Gets the access key.
     *
     * @return the access key
     */
    public String getAccessKey() {
        return accessKey;
    }
    
    /**
     * Gets the secret key.
     *
     * @return the secret key
     */
    public String getSecretKey() {
        return secretKey;
    }
    
    /**
     * Gets the S3 API URL.
     *
     * @return the S3 API URL
     */
    public String getS3URL() {
        return String.format("http://%s:%d", getHost(), getMappedPort(DEFAULT_PORT));
    }
}
