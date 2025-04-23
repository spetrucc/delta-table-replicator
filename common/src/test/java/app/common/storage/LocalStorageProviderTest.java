package app.common.storage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link LocalStorageProvider}
 */
class LocalStorageProviderTest {

    @TempDir
    Path tempDir;
    
    private LocalStorageProvider storageProvider;
    private Path basePath;
    
    @BeforeEach
    void setUp() {
        basePath = tempDir;
        storageProvider = new LocalStorageProvider(basePath.toString());
    }
    
    @AfterEach
    void tearDown() {
        storageProvider.close();
    }
    
    @Test
    void testConstructorTrimsFilePrefix() {
        LocalStorageProvider provider = new LocalStorageProvider("file://" + basePath);
        assertEquals(basePath.toString(), provider.getBasePath());
    }
    
    @Test
    void testFileExists() throws IOException {
        // Create a test file
        Path testFile = basePath.resolve("testFile.txt");
        Files.writeString(testFile, "test content");
        
        assertTrue(storageProvider.fileExists("testFile.txt"));
        assertFalse(storageProvider.fileExists("nonExistentFile.txt"));
    }
    
    @Test
    void testDirectoryExists() throws IOException {
        // Create a test directory
        Path testDir = basePath.resolve("testDir");
        Files.createDirectory(testDir);
        
        assertTrue(storageProvider.directoryExists("testDir"));
        assertFalse(storageProvider.directoryExists("nonExistentDir"));
    }
    
    @Test
    void testCreateDirectory() throws IOException {
        storageProvider.createDirectory("newDir");
        Path newDir = basePath.resolve("newDir");
        
        assertTrue(Files.exists(newDir));
        assertTrue(Files.isDirectory(newDir));
    }
    
    @Test
    void testListFiles() throws IOException {
        // Create test directory with files
        Path testDir = basePath.resolve("testListDir");
        Files.createDirectory(testDir);
        
        Files.writeString(testDir.resolve("file1.txt"), "content");
        Files.writeString(testDir.resolve("file2.txt"), "content");
        Files.writeString(testDir.resolve("file3.log"), "content");
        
        List<String> textFiles = storageProvider.listFiles("testListDir", ".*\\.txt");
        
        assertEquals(2, textFiles.size());
        List<String> fileNames = textFiles.stream()
                .map(path -> path.substring(path.lastIndexOf('/') + 1))
                .collect(Collectors.toList());
        assertTrue(fileNames.contains("file1.txt"));
        assertTrue(fileNames.contains("file2.txt"));
        assertFalse(fileNames.contains("file3.log"));
    }
    
    @Test
    void testListFilesThrowsExceptionForNonExistentDirectory() {
        assertThrows(IOException.class, () -> storageProvider.listFiles("nonExistentDir", ".*"));
    }
    
    @Test
    void testGetInputStream() throws IOException {
        // Create a test file
        String content = "test content for input stream";
        Path testFile = basePath.resolve("inputTest.txt");
        Files.writeString(testFile, content);
        
        try (var inputStream = storageProvider.getInputStream("inputTest.txt")) {
            String readContent = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            assertEquals(content, readContent);
        }
    }
    
    @Test
    void testGetOutputStream() throws IOException {
        // Test writing to a file via output stream
        String content = "test content for output stream";
        
        try (OutputStream outputStream = storageProvider.getOutputStream("outputTest.txt")) {
            outputStream.write(content.getBytes(StandardCharsets.UTF_8));
        }
        
        Path testFile = basePath.resolve("outputTest.txt");
        assertTrue(Files.exists(testFile));
        assertEquals(content, Files.readString(testFile));
    }
    
    @Test
    void testUploadFile() throws IOException {
        // Create a temp file to upload
        Path sourceFile = Files.createTempFile("upload", ".txt");
        String content = "content for upload test";
        Files.writeString(sourceFile, content);
        
        try {
            storageProvider.uploadFile(sourceFile.toString(), "uploaded.txt");
            
            Path targetFile = basePath.resolve("uploaded.txt");
            assertTrue(Files.exists(targetFile));
            assertEquals(content, Files.readString(targetFile));
        } finally {
            Files.deleteIfExists(sourceFile);
        }
    }
    
    @Test
    void testDownloadFile() throws IOException {
        // Create a file in the storage to download
        Path sourceFile = basePath.resolve("toDownload.txt");
        String content = "content for download test";
        Files.writeString(sourceFile, content);
        
        Path targetFile = Files.createTempFile("downloaded", ".txt");
        Files.delete(targetFile); // Delete so that download can create it
        
        try {
            storageProvider.downloadFile("toDownload.txt", targetFile.toString());
            
            assertTrue(Files.exists(targetFile));
            assertEquals(content, Files.readString(targetFile));
        } finally {
            Files.deleteIfExists(targetFile);
        }
    }
    
    @Test
    void testReadFileAsString() throws IOException {
        // Create a file in the storage to read
        Path testFile = basePath.resolve("readTest.txt");
        String content = "content for read test";
        Files.writeString(testFile, content);
        
        String read = storageProvider.readFileAsString("readTest.txt");
        assertEquals(content, read);
    }
    
    @Test
    void testWriteStringToFile() throws IOException {
        String content = "content for write test";
        storageProvider.writeStringToFile("writeTest.txt", content);
        
        Path testFile = basePath.resolve("writeTest.txt");
        assertTrue(Files.exists(testFile));
        assertEquals(content, Files.readString(testFile));
    }
    
    @Test
    void testGetBasePath() {
        assertEquals(basePath.toString(), storageProvider.getBasePath());
    }
    
    @Test
    void testResolvePathWithAbsolutePathMatchingBasePath() {
        String path = basePath + "/test.txt";
        assertEquals(path, storageProvider.resolvePath(path));
    }
    
    @Test
    void testResolvePathWithAbsolutePath() {
        String resolved = storageProvider.resolvePath("/test.txt");
        assertEquals(basePath + "/test.txt", resolved);
    }
    
    @Test
    void testResolvePathWithAbsolutePathAndBasePathEndingWithSlash() {
        LocalStorageProvider provider = new LocalStorageProvider(basePath + "/");
        String resolved = provider.resolvePath("/test.txt");
        assertEquals(basePath + "/test.txt", resolved);
    }
    
    @Test
    void testResolvePathWithRelativePath() {
        String resolved = storageProvider.resolvePath("test.txt");
        assertEquals(basePath + "/test.txt", resolved);
    }
    
    @Test
    void testResolvePathWithRelativePathAndBasePathEndingWithSlash() {
        LocalStorageProvider provider = new LocalStorageProvider(basePath + "/");
        String resolved = provider.resolvePath("test.txt");
        assertEquals(basePath + "/test.txt", resolved);
    }
}
