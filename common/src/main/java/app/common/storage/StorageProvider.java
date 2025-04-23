package app.common.storage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Interface for storage providers that handle file operations.
 * Implementations can support different storage systems like local filesystem or S3.
 */
public interface StorageProvider {
    
    /**
     * Checks if a file exists at the given path.
     *
     * @param path The path to check
     * @return true if the file exists, false otherwise
     * @throws IOException If an I/O error occurs
     */
    boolean fileExists(String path) throws IOException;
    
    /**
     * Checks if a directory exists at the given path.
     *
     * @param path The path to check
     * @return true if the directory exists, false otherwise
     * @throws IOException If an I/O error occurs
     */
    boolean directoryExists(String path) throws IOException;
    
    /**
     * Creates a directory at the given path.
     *
     * @param path The path to create
     * @throws IOException If an I/O error occurs
     */
    void createDirectory(String path) throws IOException;
    
    /**
     * Lists files in a directory that match a specific pattern.
     *
     * @param directoryPath The directory path to list files from
     * @param pattern A regex pattern to match filenames against
     * @return A list of matching file paths (relative to the directory)
     * @throws IOException If an I/O error occurs
     */
    List<String> listFiles(String directoryPath, String pattern) throws IOException;
    
    /**
     * Gets an input stream for reading a file.
     *
     * @param path The path to the file
     * @return An input stream for the file
     * @throws IOException If an I/O error occurs
     */
    InputStream getInputStream(String path) throws IOException;
    
    /**
     * Gets an output stream for writing to a file.
     *
     * @param path The path to the file
     * @return An output stream for the file
     * @throws IOException If an I/O error occurs
     */
    OutputStream getOutputStream(String path) throws IOException;
    
    /**
     * Uploads a file from a local path to the storage.
     *
     * @param sourcePath The local source path
     * @param targetPath The target path in the storage
     * @throws IOException If an I/O error occurs
     */
    void uploadFile(String sourcePath, String targetPath) throws IOException;
    
    /**
     * Downloads a file from the storage to a local path.
     *
     * @param sourcePath The source path in the storage
     * @param targetPath The local target path
     * @throws IOException If an I/O error occurs
     */
    void downloadFile(String sourcePath, String targetPath) throws IOException;
    
    /**
     * Reads the content of a text file as a string.
     *
     * @param path The path to the file
     * @return The content of the file as a string
     * @throws IOException If an I/O error occurs
     */
    String readFileAsString(String path) throws IOException;
    
    /**
     * Writes a string to a text file.
     *
     * @param path The path to the file
     * @param content The content to write
     * @throws IOException If an I/O error occurs
     */
    void writeStringToFile(String path, String content) throws IOException;
    
    /**
     * Gets the base path for this storage provider.
     *
     * @return The base path
     */
    String getBasePath();
    
    /**
     * Resolves a path relative to the base path.
     *
     * @param relativePath The relative path
     * @return The full resolved path
     */
    String resolvePath(String relativePath);
    
    /**
     * Closes any resources used by this storage provider.
     */
    void close();
}
