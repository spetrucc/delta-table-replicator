package app.common.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Storage provider implementation for local filesystem.
 */
public class LocalStorageProvider implements StorageProvider {
    private static final Logger LOG = LoggerFactory.getLogger(LocalStorageProvider.class);
    private static final int BUFFER_SIZE = 8192;
    
    private final String basePath;
    
    /**
     * Creates a new LocalStorageProvider.
     *
     * @param basePath The base path for this storage provider
     */
    public LocalStorageProvider(String basePath) {
        // Remove file:// prefix if present
        if (basePath.startsWith("file://")) {
            this.basePath = basePath.substring(7);
        } else {
            this.basePath = basePath;
        }
        LOG.info("Initialized local storage provider with base path: {}", this.basePath);
    }
    
    @Override
    public boolean fileExists(String path) throws IOException {
        Path filePath = Paths.get(resolvePath(path));
        return Files.exists(filePath) && Files.isRegularFile(filePath);
    }
    
    @Override
    public boolean directoryExists(String path) throws IOException {
        Path dirPath = Paths.get(resolvePath(path));
        return Files.exists(dirPath) && Files.isDirectory(dirPath);
    }
    
    @Override
    public void createDirectory(String path) throws IOException {
        Path dirPath = Paths.get(resolvePath(path));
        Files.createDirectories(dirPath);
    }
    
    @Override
    public List<String> listFiles(String directoryPath, String pattern) throws IOException {
        Path dirPath = Paths.get(resolvePath(directoryPath));
        if (!Files.exists(dirPath) || !Files.isDirectory(dirPath)) {
            throw new IOException("Directory does not exist: " + dirPath);
        }
        
        try (Stream<Path> stream = Files.list(dirPath)) {
            return stream
                    .filter(Files::isRegularFile)
                    .map(Path::getFileName)
                    .map(Path::toString)
                    .filter(name -> name.matches(pattern))
                    .map(name -> directoryPath + "/" + name)
                    .collect(Collectors.toList());
        }
    }
    
    @Override
    public InputStream getInputStream(String path) throws IOException {
        return new FileInputStream(resolvePath(path));
    }
    
    @Override
    public OutputStream getOutputStream(String path) throws IOException {
        Path filePath = Paths.get(resolvePath(path));
        Files.createDirectories(filePath.getParent());
        return new FileOutputStream(filePath.toFile());
    }
    
    @Override
    public void uploadFile(String sourcePath, String targetPath) throws IOException {
        Path source = Paths.get(sourcePath);
        Path target = Paths.get(resolvePath(targetPath));
        
        // Create parent directories if they don't exist
        Files.createDirectories(target.getParent());
        
        // Copy the file
        Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
    }
    
    @Override
    public void downloadFile(String sourcePath, String targetPath) throws IOException {
        Path source = Paths.get(resolvePath(sourcePath));
        Path target = Paths.get(targetPath);
        
        // Create parent directories if they don't exist
        Files.createDirectories(target.getParent());
        
        // Copy the file
        Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
    }
    
    @Override
    public String readFileAsString(String path) throws IOException {
        return Files.readString(Paths.get(resolvePath(path)));
    }
    
    @Override
    public void writeStringToFile(String path, String content) throws IOException {
        Path filePath = Paths.get(resolvePath(path));
        Files.createDirectories(filePath.getParent());
        Files.writeString(filePath, content, StandardCharsets.UTF_8);
    }
    
    @Override
    public String getBasePath() {
        return basePath;
    }
    
    @Override
    public String resolvePath(String relativePath) {
        // Handle absolute paths that match our base path prefix
        if (relativePath.startsWith(basePath)) {
            return relativePath;
        }

        // Handle absolute paths
        if (relativePath.startsWith("/")) {
            // Make sure we don't add double slashes
            if (basePath.endsWith("/")) {
                return basePath + relativePath.substring(1);
            }
            return basePath + relativePath;
        }
        
        // Handle relative paths
        // Make sure we don't add double slashes
        if (basePath.endsWith("/")) {
            return basePath + relativePath;
        }
        return basePath + "/" + relativePath;
    }
    
    @Override
    public void close() {
        // No resources to close for local storage
    }
}
