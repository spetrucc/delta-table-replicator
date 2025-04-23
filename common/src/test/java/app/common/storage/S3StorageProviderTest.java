package app.common.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link S3StorageProvider}
 * Uses a custom TestS3Client implementation to simulate S3 operations.
 */
class S3StorageProviderTest {
    private static final String TEST_BUCKET = "test-bucket";
    private static final String TEST_PREFIX = "test-prefix";
    private static final String TEST_BASE_PATH = "s3://" + TEST_BUCKET + "/" + TEST_PREFIX;

    @TempDir
    Path tempDir;

    private S3StorageProvider storageProvider;
    private TestS3Client s3TestClient;

    @BeforeEach
    void setUp() throws Exception {
        // Create the test S3Client
        s3TestClient = new TestS3Client();
        
        // Create the provider to test
        storageProvider = new S3StorageProvider(
                TEST_BASE_PATH,
                "testAccessKey",
                "testSecretKey",
                "http://localhost:9000",
                true
        );
        
        // Inject our test client using reflection
        Field s3ClientField = S3StorageProvider.class.getDeclaredField("s3Client");
        s3ClientField.setAccessible(true);
        s3ClientField.set(storageProvider, s3TestClient);
    }

    @AfterEach
    void tearDown() {
        if (storageProvider != null) {
            storageProvider.close();
        }
    }

    @Test
    void testConstructorWithS3Prefix() throws Exception {
        S3StorageProvider provider = new S3StorageProvider(
                "s3://" + TEST_BUCKET + "/" + TEST_PREFIX,
                "testAccessKey",
                "testSecretKey",
                "http://localhost:9000",
                true
        );
        
        // Inject our test client
        Field s3ClientField = S3StorageProvider.class.getDeclaredField("s3Client");
        s3ClientField.setAccessible(true);
        s3ClientField.set(provider, s3TestClient);
        
        assertEquals("s3://" + TEST_BUCKET + "/" + TEST_PREFIX, provider.getBasePath());
    }

    @Test
    void testConstructorWithS3aPrefix() throws Exception {
        S3StorageProvider provider = new S3StorageProvider(
                "s3a://" + TEST_BUCKET + "/" + TEST_PREFIX,
                "testAccessKey",
                "testSecretKey",
                "http://localhost:9000",
                true
        );
        
        // Inject our test client
        Field s3ClientField = S3StorageProvider.class.getDeclaredField("s3Client");
        s3ClientField.setAccessible(true);
        s3ClientField.set(provider, s3TestClient);
        
        assertEquals("s3a://" + TEST_BUCKET + "/" + TEST_PREFIX, provider.getBasePath());
    }

    @Test
    void testFileExists() throws IOException {
        // Prepare test data in our S3 test client
        s3TestClient.putObject(
                software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
                        .bucket(TEST_BUCKET)
                        .key(TEST_PREFIX + "/test-file.txt")
                        .build(),
                software.amazon.awssdk.core.sync.RequestBody.fromString("test content", StandardCharsets.UTF_8)
        );

        assertTrue(storageProvider.fileExists("test-file.txt"));
        assertFalse(storageProvider.fileExists("non-existent-file.txt"));
    }

    @Test
    void testDirectoryExists() throws IOException {
        // Create a file in a "directory"
        s3TestClient.putObject(
                software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
                        .bucket(TEST_BUCKET)
                        .key(TEST_PREFIX + "/test-dir/file.txt")
                        .build(),
                software.amazon.awssdk.core.sync.RequestBody.fromString("test content", StandardCharsets.UTF_8)
        );

        assertTrue(storageProvider.directoryExists("test-dir"));
        assertFalse(storageProvider.directoryExists("non-existent-dir"));
    }

    @Test
    void testCreateDirectory() throws IOException {
        storageProvider.createDirectory("new-dir");

        // Verify the directory marker object is created
        assertTrue(s3TestClient.headObject(
                software.amazon.awssdk.services.s3.model.HeadObjectRequest.builder()
                        .bucket(TEST_BUCKET)
                        .key(TEST_PREFIX + "/new-dir/")
                        .build()
        ) != null);
    }

    @Test
    void testListFiles() throws IOException {
        // Create test files
        s3TestClient.putObject(
                software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
                        .bucket(TEST_BUCKET)
                        .key(TEST_PREFIX + "/test-dir/file1.txt")
                        .build(),
                software.amazon.awssdk.core.sync.RequestBody.fromString("test content", StandardCharsets.UTF_8)
        );
        s3TestClient.putObject(
                software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
                        .bucket(TEST_BUCKET)
                        .key(TEST_PREFIX + "/test-dir/file2.txt")
                        .build(),
                software.amazon.awssdk.core.sync.RequestBody.fromString("test content", StandardCharsets.UTF_8)
        );
        s3TestClient.putObject(
                software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
                        .bucket(TEST_BUCKET)
                        .key(TEST_PREFIX + "/test-dir/file3.log")
                        .build(),
                software.amazon.awssdk.core.sync.RequestBody.fromString("test content", StandardCharsets.UTF_8)
        );

        // List only text files
        List<String> textFiles = storageProvider.listFiles("test-dir", ".*\\.txt");

        assertEquals(2, textFiles.size());
        List<String> fileNames = textFiles.stream()
                .map(path -> path.substring(path.lastIndexOf('/') + 1))
                .collect(Collectors.toList());
        assertTrue(fileNames.contains("file1.txt"));
        assertTrue(fileNames.contains("file2.txt"));
        assertFalse(fileNames.contains("file3.log"));
    }

    @Test
    void testGetInputStream() throws IOException {
        // Create test file in S3
        String content = "test content for input stream";
        s3TestClient.putObject(
                software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
                        .bucket(TEST_BUCKET)
                        .key(TEST_PREFIX + "/input-test.txt")
                        .build(),
                software.amazon.awssdk.core.sync.RequestBody.fromString(content, StandardCharsets.UTF_8)
        );

        // Read using InputStream
        try (InputStream is = storageProvider.getInputStream("input-test.txt")) {
            String readContent = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            assertEquals(content, readContent);
        }
    }

    @Test
    void testGetOutputStream() throws IOException {
        // Write content using OutputStream
        String content = "test content for output stream";
        try (OutputStream os = storageProvider.getOutputStream("output-test.txt")) {
            os.write(content.getBytes(StandardCharsets.UTF_8));
        }

        // Verify content in S3
        try (InputStream is = s3TestClient.getObject(
                software.amazon.awssdk.services.s3.model.GetObjectRequest.builder()
                        .bucket(TEST_BUCKET)
                        .key(TEST_PREFIX + "/output-test.txt")
                        .build()
        )) {
            String retrievedContent = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            assertEquals(content, retrievedContent);
        }
    }

    @Test
    void testUploadFile() throws IOException {
        // Create a temp file to upload
        Path sourceFile = Files.createTempFile(tempDir, "upload", ".txt");
        String content = "content for upload test";
        Files.writeString(sourceFile, content);

        // Upload to S3
        storageProvider.uploadFile(sourceFile.toString(), "uploaded.txt");

        // Verify content in S3
        try (InputStream is = s3TestClient.getObject(
                software.amazon.awssdk.services.s3.model.GetObjectRequest.builder()
                        .bucket(TEST_BUCKET)
                        .key(TEST_PREFIX + "/uploaded.txt")
                        .build()
        )) {
            String retrievedContent = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            assertEquals(content, retrievedContent);
        }
    }

    @Test
    void testDownloadFile() throws IOException {
        // Create a file in S3 to download
        String content = "content for download test";
        s3TestClient.putObject(
                software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
                        .bucket(TEST_BUCKET)
                        .key(TEST_PREFIX + "/to-download.txt")
                        .build(),
                software.amazon.awssdk.core.sync.RequestBody.fromString(content, StandardCharsets.UTF_8)
        );

        // Download the file
        Path targetFile = tempDir.resolve("downloaded.txt");

        storageProvider.downloadFile("to-download.txt", targetFile.toString());

        // Verify content
        assertTrue(Files.exists(targetFile));
        assertEquals(content, Files.readString(targetFile));
    }

    @Test
    void testReadFileAsString() throws IOException {
        // Create a file in S3
        String content = "content for read test";
        s3TestClient.putObject(
                software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
                        .bucket(TEST_BUCKET)
                        .key(TEST_PREFIX + "/read-test.txt")
                        .build(),
                software.amazon.awssdk.core.sync.RequestBody.fromString(content, StandardCharsets.UTF_8)
        );

        // Read the file
        String read = storageProvider.readFileAsString("read-test.txt");
        assertEquals(content, read);
    }

    @Test
    void testWriteStringToFile() throws IOException {
        String content = "content for write test";
        storageProvider.writeStringToFile("write-test.txt", content);

        // Verify content in S3
        try (InputStream is = s3TestClient.getObject(
                software.amazon.awssdk.services.s3.model.GetObjectRequest.builder()
                        .bucket(TEST_BUCKET)
                        .key(TEST_PREFIX + "/write-test.txt")
                        .build()
        )) {
            String retrievedContent = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            assertEquals(content, retrievedContent);
        }
    }

    @Test
    void testResolvePath() {
        assertEquals("s3a://" + TEST_BUCKET + "/test-prefix/test.txt", 
                storageProvider.resolvePath("test.txt"));
        assertEquals("s3a://" + TEST_BUCKET + "/test.txt", 
                storageProvider.resolvePath("/test.txt"));
    }
}
