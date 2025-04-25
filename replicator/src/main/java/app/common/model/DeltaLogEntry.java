package app.common.model;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

/**
 * Represents a Delta log entry with operations like add, remove, etc.
 */
public class DeltaLogEntry {
    @Expose
    private AddOperation add;
    @Expose
    private RemoveOperation remove;
    @Expose
    private CommitInfo commitInfo;
    @Expose
    private MetadataOperation metaData;
    @Expose
    private ProtocolOperation protocol;

    public AddOperation getAdd() {
        return add;
    }

    public void setAdd(AddOperation add) {
        this.add = add;
    }

    public RemoveOperation getRemove() {
        return remove;
    }

    public void setRemove(RemoveOperation remove) {
        this.remove = remove;
    }

    public CommitInfo getCommitInfo() {
        return commitInfo;
    }

    public void setCommitInfo(CommitInfo commitInfo) {
        this.commitInfo = commitInfo;
    }

    public MetadataOperation getMetaData() {
        return metaData;
    }

    public void setMetaData(MetadataOperation metaData) {
        this.metaData = metaData;
    }

    public ProtocolOperation getProtocol() {
        return protocol;
    }

    public void setProtocol(ProtocolOperation protocol) {
        this.protocol = protocol;
    }

    /**
     * Represents an add operation in a Delta log.
     */
    public static class AddOperation {
        @Expose
        private String path;
        @Expose
        private Long size;
        @Expose
        private DeletionVector deletionVector;
        @Expose
        private DeletionVectorDescriptor deletionVectorDescriptor;
        @Expose
        private String dataChange;
        
        public String getPath() {
            return path;
        }
        
        public void setPath(String path) {
            this.path = path;
        }
        
        public Long getSize() {
            return size;
        }
        
        public void setSize(Long size) {
            this.size = size;
        }
        
        public DeletionVector getDeletionVector() {
            return deletionVector;
        }
        
        public void setDeletionVector(DeletionVector deletionVector) {
            this.deletionVector = deletionVector;
        }
        
        public DeletionVectorDescriptor getDeletionVectorDescriptor() {
            return deletionVectorDescriptor;
        }
        
        public void setDeletionVectorDescriptor(DeletionVectorDescriptor deletionVectorDescriptor) {
            this.deletionVectorDescriptor = deletionVectorDescriptor;
        }
        
        public String getDataChange() {
            return dataChange;
        }
        
        public void setDataChange(String dataChange) {
            this.dataChange = dataChange;
        }
    }

    /**
     * Represents a deletion vector in an add operation.
     */
    public static class DeletionVector {
        @SerializedName("dvFile")
        @Expose
        private String dvFile;
        @SerializedName("storagePath")
        @Expose
        private String storagePath;
        @SerializedName("pathOrInlineDv")
        @Expose
        private String pathOrInlineDv;
        @SerializedName("storageType")
        @Expose
        private String storageType;
        @SerializedName("offset")
        @Expose
        private Integer offset;
        @SerializedName("sizeInBytes")
        @Expose
        private Integer sizeInBytes;
        @SerializedName("cardinality")
        @Expose
        private Integer cardinality;
        
        public String getDvFile() {
            return dvFile;
        }
        
        public void setDvFile(String dvFile) {
            this.dvFile = dvFile;
        }
        
        public String getStoragePath() {
            return storagePath;
        }
        
        public void setStoragePath(String storagePath) {
            this.storagePath = storagePath;
        }
        
        public String getPathOrInlineDv() {
            return pathOrInlineDv;
        }
        
        public void setPathOrInlineDv(String pathOrInlineDv) {
            this.pathOrInlineDv = pathOrInlineDv;
        }
        
        public String getStorageType() {
            return storageType;
        }
        
        public void setStorageType(String storageType) {
            this.storageType = storageType;
        }
        
        public Integer getOffset() {
            return offset;
        }
        
        public void setOffset(Integer offset) {
            this.offset = offset;
        }
        
        public Integer getSizeInBytes() {
            return sizeInBytes;
        }
        
        public void setSizeInBytes(Integer sizeInBytes) {
            this.sizeInBytes = sizeInBytes;
        }
        
        public Integer getCardinality() {
            return cardinality;
        }
        
        public void setCardinality(Integer cardinality) {
            this.cardinality = cardinality;
        }
    }

    /**
     * Represents a deletion vector descriptor.
     */
    public static class DeletionVectorDescriptor {
        @SerializedName("format")
        @Expose
        private String format;
        @SerializedName("numRows")
        @Expose
        private Integer numRows;
        @SerializedName("storagePath")
        @Expose
        private String storagePath;
        
        public String getFormat() {
            return format;
        }
        
        public void setFormat(String format) {
            this.format = format;
        }
        
        public Integer getNumRows() {
            return numRows;
        }
        
        public void setNumRows(Integer numRows) {
            this.numRows = numRows;
        }
        
        public String getStoragePath() {
            return storagePath;
        }
        
        public void setStoragePath(String storagePath) {
            this.storagePath = storagePath;
        }
    }

    /**
     * Represents a remove operation in a Delta log.
     */
    public static class RemoveOperation {
        @Expose
        private String path;
        @Expose
        private Long deletionTimestamp;
        @Expose
        private Boolean dataChange;
        @Expose
        private Boolean extendedFileMetadata;
        @Expose
        private Object partitionValues;
        @Expose
        private Long size;
        @Expose
        private DeletionVector deletionVector;
        
        public String getPath() {
            return path;
        }
        
        public void setPath(String path) {
            this.path = path;
        }
        
        public Long getDeletionTimestamp() {
            return deletionTimestamp;
        }
        
        public void setDeletionTimestamp(Long deletionTimestamp) {
            this.deletionTimestamp = deletionTimestamp;
        }
        
        public Boolean getDataChange() {
            return dataChange;
        }
        
        public void setDataChange(Boolean dataChange) {
            this.dataChange = dataChange;
        }
        
        public Boolean getExtendedFileMetadata() {
            return extendedFileMetadata;
        }
        
        public void setExtendedFileMetadata(Boolean extendedFileMetadata) {
            this.extendedFileMetadata = extendedFileMetadata;
        }
        
        public Object getPartitionValues() {
            return partitionValues;
        }
        
        public void setPartitionValues(Object partitionValues) {
            this.partitionValues = partitionValues;
        }
        
        public Long getSize() {
            return size;
        }
        
        public void setSize(Long size) {
            this.size = size;
        }
        
        public DeletionVector getDeletionVector() {
            return deletionVector;
        }
        
        public void setDeletionVector(DeletionVector deletionVector) {
            this.deletionVector = deletionVector;
        }
    }

    /**
     * Represents commit info in a Delta log.
     */
    public static class CommitInfo {
        // Add fields as needed
    }

    /**
     * Represents metadata operation in a Delta log.
     */
    public static class MetadataOperation {
        // Add fields as needed
    }

    /**
     * Represents protocol operation in a Delta log.
     */
    public static class ProtocolOperation {
        // Add fields as needed
    }
}
