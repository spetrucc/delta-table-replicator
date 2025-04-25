package app.common.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link S3StorageProvider} using mocked S3Client.
 */
@ExtendWith(MockitoExtension.class)
class S3StorageProviderMockTest {
    private static final String TEST_BUCKET = "test-bucket";
    private static final String TEST_PREFIX = "test-prefix";
    private static final String TEST_BASE_PATH = "s3://" + TEST_BUCKET + "/" + TEST_PREFIX;
    
    @Mock
    private S3Client s3ClientMock;
    
    private S3StorageProvider storageProvider;
    
    @TempDir
    Path tempDir;
    
    @BeforeEach
    void setUp() throws Exception {
        // Create provider with test base path
        storageProvider = new S3StorageProvider(
                TEST_BASE_PATH,
                "testAccessKey",
                "testSecretKey",
                "http://localhost:9000",
                true
        );
        
        // Use reflection to inject the mock S3Client
        Field s3ClientField = S3StorageProvider.class.getDeclaredField("s3Client");
        s3ClientField.setAccessible(true);
        s3ClientField.set(storageProvider, s3ClientMock);
    }
    
    @Test
    void testFileExists() throws IOException {
        // Set up mock to return success for existing file
        when(s3ClientMock.headObject(any(HeadObjectRequest.class)))
                .thenReturn(HeadObjectResponse.builder().build());
        
        assertTrue(storageProvider.fileExists("test-file.txt"));
        
        // Verify the correct key was used
        ArgumentCaptor<HeadObjectRequest> requestCaptor = ArgumentCaptor.forClass(HeadObjectRequest.class);
        verify(s3ClientMock).headObject(requestCaptor.capture());
        assertEquals(TEST_BUCKET, requestCaptor.getValue().bucket());
        assertEquals(TEST_PREFIX + "/test-file.txt", requestCaptor.getValue().key());
        
        // Set up mock to throw exception for non-existent file
        when(s3ClientMock.headObject(any(HeadObjectRequest.class)))
                .thenThrow(NoSuchKeyException.builder().build());
        
        assertFalse(storageProvider.fileExists("non-existent-file.txt"));
    }
    
    @Test
    void testDirectoryExists() throws IOException {
        // Set up mock for existing directory
        ListObjectsV2Response existingDirResponse = ListObjectsV2Response.builder()
                .contents(S3Object.builder().key(TEST_PREFIX + "/test-dir/file.txt").build())
                .build();
        when(s3ClientMock.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenReturn(existingDirResponse);
        
        assertTrue(storageProvider.directoryExists("test-dir"));
        
        // Verify the correct prefix was used
        ArgumentCaptor<ListObjectsV2Request> requestCaptor = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        verify(s3ClientMock).listObjectsV2(requestCaptor.capture());
        assertEquals(TEST_BUCKET, requestCaptor.getValue().bucket());
        assertEquals(TEST_PREFIX + "/test-dir/", requestCaptor.getValue().prefix());
        
        // Set up mock for non-existent directory
        ListObjectsV2Response emptyResponse = ListObjectsV2Response.builder().build();
        when(s3ClientMock.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenReturn(emptyResponse);
        
        assertFalse(storageProvider.directoryExists("non-existent-dir"));
    }
    
    @Test
    void testCreateDirectory() throws IOException {
        storageProvider.createDirectory("new-dir");
        
        // Verify correct S3 putObject call was made
        ArgumentCaptor<PutObjectRequest> requestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(s3ClientMock).putObject(requestCaptor.capture(), bodyCaptor.capture());
        
        assertEquals(TEST_BUCKET, requestCaptor.getValue().bucket());
        assertEquals(TEST_PREFIX + "/new-dir/", requestCaptor.getValue().key());
        assertEquals(0, bodyCaptor.getValue().contentLength());
    }
    
    @Test
    void testListFiles() throws IOException {
        // Set up mock response with files
        List<S3Object> objects = new ArrayList<>();
        objects.add(S3Object.builder().key(TEST_PREFIX + "/test-dir/file1.txt").build());
        objects.add(S3Object.builder().key(TEST_PREFIX + "/test-dir/file2.txt").build());
        objects.add(S3Object.builder().key(TEST_PREFIX + "/test-dir/file3.log").build());
        
        ListObjectsV2Response response = ListObjectsV2Response.builder()
                .contents(objects)
                .build();
        
        when(s3ClientMock.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenReturn(response);
        
        // Test listing only .txt files
        List<String> textFiles = storageProvider.listFiles("test-dir", ".*\\.txt");
        
        assertEquals(2, textFiles.size());
        assertTrue(textFiles.contains("test-dir/file1.txt"));
        assertTrue(textFiles.contains("test-dir/file2.txt"));
        assertFalse(textFiles.contains("test-dir/file3.log"));
        
        // Verify correct request
        ArgumentCaptor<ListObjectsV2Request> requestCaptor = ArgumentCaptor.forClass(ListObjectsV2Request.class);
        verify(s3ClientMock).listObjectsV2(requestCaptor.capture());
        assertEquals(TEST_BUCKET, requestCaptor.getValue().bucket());
        assertEquals(TEST_PREFIX + "/test-dir/", requestCaptor.getValue().prefix());
    }
    
    @Test
    void testGetInputStream() throws IOException {
        // Mock a response input stream with test content
        byte[] content = "test content".getBytes(StandardCharsets.UTF_8);
        InputStream contentStream = new ByteArrayInputStream(content);
        ResponseInputStream<GetObjectResponse> responseStream = new ResponseInputStream<>(
                GetObjectResponse.builder().build(),
                new BufferedInputStream(contentStream)
        );
        
        when(s3ClientMock.getObject(any(GetObjectRequest.class)))
                .thenReturn(responseStream);
        
        // Get and read the input stream
        try (InputStream is = storageProvider.getInputStream("test-file.txt")) {
            String readContent = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            assertEquals("test content", readContent);
        }
        
        // Verify correct request
        ArgumentCaptor<GetObjectRequest> requestCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(s3ClientMock).getObject(requestCaptor.capture());
        assertEquals(TEST_BUCKET, requestCaptor.getValue().bucket());
        assertEquals(TEST_PREFIX + "/test-file.txt", requestCaptor.getValue().key());
    }
    
    @Test
    @SuppressWarnings("unchecked")
    void testDownloadFile() throws IOException {
        // Create a target file
        Path targetFile = tempDir.resolve("downloaded.txt");
        
        // Call the method
        storageProvider.downloadFile("source-file.txt", targetFile.toString());
        
        // Verify correct request
        ArgumentCaptor<GetObjectRequest> requestCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        ArgumentCaptor<ResponseTransformer<GetObjectResponse, ?>> transformerCaptor = 
                ArgumentCaptor.forClass(ResponseTransformer.class);
        
        verify(s3ClientMock).getObject(requestCaptor.capture(), transformerCaptor.capture());
        assertEquals(TEST_BUCKET, requestCaptor.getValue().bucket());
        assertEquals(TEST_PREFIX + "/source-file.txt", requestCaptor.getValue().key());
    }
    
    @Test
    void testUploadFile() throws IOException {
        // Create a source file
        Path sourceFile = Files.createTempFile(tempDir, "upload", ".txt");
        Files.writeString(sourceFile, "test content");
        
        // Call the method
        storageProvider.uploadFile(sourceFile.toString(), "target-file.txt");
        
        // Verify correct request
        ArgumentCaptor<PutObjectRequest> requestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(s3ClientMock).putObject(requestCaptor.capture(), any(RequestBody.class));
        assertEquals(TEST_BUCKET, requestCaptor.getValue().bucket());
        assertEquals(TEST_PREFIX + "/target-file.txt", requestCaptor.getValue().key());
    }
    
    @Test
    void testReadFileAsString() throws IOException {
        // Mock a response input stream with test content
        byte[] content = "test content".getBytes(StandardCharsets.UTF_8);
        InputStream contentStream = new ByteArrayInputStream(content);
        ResponseInputStream<GetObjectResponse> responseStream = new ResponseInputStream<>(
                GetObjectResponse.builder().build(),
                new BufferedInputStream(contentStream)
        );
        
        when(s3ClientMock.getObject(any(GetObjectRequest.class)))
                .thenReturn(responseStream);
        
        // Read the file
        String readContent = storageProvider.readFileAsString("test-file.txt");
        assertEquals("test content", readContent);
        
        // Verify correct request
        ArgumentCaptor<GetObjectRequest> requestCaptor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(s3ClientMock).getObject(requestCaptor.capture());
        assertEquals(TEST_BUCKET, requestCaptor.getValue().bucket());
        assertEquals(TEST_PREFIX + "/test-file.txt", requestCaptor.getValue().key());
    }
    
    @Test
    void testWriteStringToFile() throws IOException {
        // Call the method
        storageProvider.writeStringToFile("test-file.txt", "test content");
        
        // Verify correct request
        ArgumentCaptor<PutObjectRequest> requestCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        ArgumentCaptor<RequestBody> bodyCaptor = ArgumentCaptor.forClass(RequestBody.class);
        verify(s3ClientMock).putObject(requestCaptor.capture(), bodyCaptor.capture());
        assertEquals(TEST_BUCKET, requestCaptor.getValue().bucket());
        assertEquals(TEST_PREFIX + "/test-file.txt", requestCaptor.getValue().key());
        
        // Unfortunately, we can't easily verify the content of the RequestBody 
        // since it doesn't expose its content directly
    }
    
    @Test
    void testResolvePath() {
        // Test relative path
        String relativeResolved = storageProvider.resolvePath("file.txt");
        assertEquals("s3a://" + TEST_BUCKET + "/" + TEST_PREFIX + "/file.txt", relativeResolved);
        
        // Test absolute path
        String absoluteResolved = storageProvider.resolvePath("/file.txt");
        assertEquals("s3a://" + TEST_BUCKET + "/file.txt", absoluteResolved);
    }
    
    @Test
    void testGetBasePath() {
        assertEquals(TEST_BASE_PATH, storageProvider.getBasePath());
    }
    
    @Test
    void testCloseClosesS3Client() {
        storageProvider.close();
        verify(s3ClientMock).close();
    }
}
