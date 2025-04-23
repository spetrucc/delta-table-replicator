package app.common.model;

import java.time.Instant;

/**
 * Contains metadata about a Delta table export.
 */
public class ExportMetadata {
    private String tablePath;
    private String tableName;
    private long fromVersion;
    private long toVersion;
    private String exportTimestamp;

    /**
     * Default constructor for Gson
     */
    public ExportMetadata() {
    }

    /**
     * Creates a new ExportMetadata instance.
     *
     * @param tablePath The path to the exported Delta table
     * @param fromVersion The starting version that was exported
     * @param toVersion The ending version that was exported
     */
    public ExportMetadata(String tablePath, long fromVersion, long toVersion) {
        this.tablePath = tablePath;
        this.fromVersion = fromVersion;
        this.toVersion = toVersion;
        this.exportTimestamp = Instant.now().toString();
        
        // Extract table name from path
        if (tablePath != null) {
            String[] parts = tablePath.split("/");
            this.tableName = parts.length > 0 ? parts[parts.length - 1] : "";
        } else {
            this.tableName = "";
        }
    }

    public String getTablePath() {
        return tablePath;
    }

    public void setTablePath(String tablePath) {
        this.tablePath = tablePath;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public long getFromVersion() {
        return fromVersion;
    }

    public void setFromVersion(long fromVersion) {
        this.fromVersion = fromVersion;
    }

    public long getToVersion() {
        return toVersion;
    }

    public void setToVersion(long toVersion) {
        this.toVersion = toVersion;
    }

    public String getExportTimestamp() {
        return exportTimestamp;
    }

    public void setExportTimestamp(String exportTimestamp) {
        this.exportTimestamp = exportTimestamp;
    }

    @Override
    public String toString() {
        return "ExportMetadata{" +
                "tablePath='" + tablePath + '\'' +
                ", tableName='" + tableName + '\'' +
                ", fromVersion=" + fromVersion +
                ", toVersion=" + toVersion +
                ", exportTimestamp='" + exportTimestamp + '\'' +
                '}';
    }
}
