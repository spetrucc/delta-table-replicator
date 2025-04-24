package app.common.storage;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ServiceClientConfiguration;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A test implementation of S3Client for unit testing S3StorageProvider.
 * Only implements the methods that are actually used by S3StorageProvider.
 */
public class TestS3Client implements S3Client {
    private final Map<String, byte[]> objects = new HashMap<>();
    private final Map<String, List<String>> objectsByPrefix = new HashMap<>();
    
    /**
     * Records an object creation, both in the objects map and in the prefix mapping
     */
    private void putObjectInMaps(String key, byte[] content) {
        objects.put(key, content);
        
        // Add to prefixes
        String currentPrefix = "";
        for (String part : key.split("/")) {
            if (!currentPrefix.isEmpty()) {
                currentPrefix += "/";
            }
            currentPrefix += part;
            
            objectsByPrefix.computeIfAbsent(currentPrefix, p -> new ArrayList<>())
                    .add(key);
        }
        
        // Also add the full path as a prefix
        objectsByPrefix.computeIfAbsent(key, p -> new ArrayList<>());
    }
    
    @Override
    public HeadObjectResponse headObject(HeadObjectRequest request) {
        String key = request.key();
        if (objects.containsKey(key)) {
            return HeadObjectResponse.builder()
                    .contentLength((long) objects.get(key).length)
                    .build();
        }
        throw NoSuchKeyException.builder()
                .message("Key does not exist: " + key)
                .build();
    }
    
    @Override
    public ListObjectsV2Response listObjectsV2(ListObjectsV2Request request) {
        String prefix = request.prefix();
        if (prefix == null) {
            prefix = "";
        }
        
        List<String> keysWithPrefix = objectsByPrefix.getOrDefault(prefix.endsWith("/") ? 
                prefix.substring(0, prefix.length() - 1) : prefix, new ArrayList<>());
        
        if (keysWithPrefix.isEmpty() && !prefix.isEmpty()) {
            // Try with trailing slash
            keysWithPrefix = objectsByPrefix.getOrDefault(prefix + "/", new ArrayList<>());
        }
        
        List<S3Object> contents = new ArrayList<>();
        for (String key : keysWithPrefix) {
            if (key.startsWith(prefix)) {
                contents.add(S3Object.builder()
                        .key(key)
                        .size((long) objects.getOrDefault(key, new byte[0]).length)
                        .build());
            }
        }
        
        return ListObjectsV2Response.builder()
                .contents(contents)
                .build();
    }
    
    @Override
    public PutObjectResponse putObject(PutObjectRequest request, RequestBody requestBody) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            InputStream is = requestBody.contentStreamProvider().newStream();
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = is.read(buffer)) != -1) {
                baos.write(buffer, 0, bytesRead);
            }
            putObjectInMaps(request.key(), baos.toByteArray());
            return PutObjectResponse.builder().build();
        } catch (IOException e) {
            throw new RuntimeException("Failed to write test object", e);
        }
    }
    
    @Override
    public ResponseInputStream<GetObjectResponse> getObject(GetObjectRequest request) {
        String key = request.key();
        if (objects.containsKey(key)) {
            byte[] content = objects.get(key);
            ByteArrayInputStream inputStream = new ByteArrayInputStream(content);
            GetObjectResponse response = GetObjectResponse.builder()
                    .contentLength((long) content.length)
                    .build();
            return new ResponseInputStream<>(response, inputStream);
        }
        throw NoSuchKeyException.builder()
                .message("Key does not exist: " + key)
                .build();
    }
    
    @Override
    public <T> T getObject(GetObjectRequest request, ResponseTransformer<GetObjectResponse, T> responseTransformer) {
        ResponseInputStream<GetObjectResponse> responseInputStream = getObject(request);
        try {
            return responseTransformer.transform(responseInputStream.response(), AbortableInputStream.create(responseInputStream));
        } catch (Exception e) {
            throw new RuntimeException("Failed to transform response", e);
        }
    }
    
    @Override
    public CreateBucketResponse createBucket(CreateBucketRequest request) {
        // Just return success
        return CreateBucketResponse.builder().build();
    }
    
    @Override
    public DeleteObjectResponse deleteObject(DeleteObjectRequest request) {
        objects.remove(request.key());
        return DeleteObjectResponse.builder().build();
    }
    
    // Stub implementations of S3Client interface
    
    @Override
    public String serviceName() {
        return "s3";
    }
    
    @Override
    public void close() {
        // No-op
    }
    
    // The following methods are not used by S3StorageProvider, but are required by the interface
    
    @Override
    public AbortMultipartUploadResponse abortMultipartUpload(AbortMultipartUploadRequest abortMultipartUploadRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public CompleteMultipartUploadResponse completeMultipartUpload(CompleteMultipartUploadRequest completeMultipartUploadRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public CopyObjectResponse copyObject(CopyObjectRequest copyObjectRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public DeleteBucketResponse deleteBucket(DeleteBucketRequest deleteBucketRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public DeleteBucketAnalyticsConfigurationResponse deleteBucketAnalyticsConfiguration(DeleteBucketAnalyticsConfigurationRequest deleteBucketAnalyticsConfigurationRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public DeleteBucketCorsResponse deleteBucketCors(DeleteBucketCorsRequest deleteBucketCorsRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public DeleteBucketEncryptionResponse deleteBucketEncryption(DeleteBucketEncryptionRequest deleteBucketEncryptionRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public DeleteBucketIntelligentTieringConfigurationResponse deleteBucketIntelligentTieringConfiguration(DeleteBucketIntelligentTieringConfigurationRequest deleteBucketIntelligentTieringConfigurationRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public DeleteBucketInventoryConfigurationResponse deleteBucketInventoryConfiguration(DeleteBucketInventoryConfigurationRequest deleteBucketInventoryConfigurationRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public DeleteBucketLifecycleResponse deleteBucketLifecycle(DeleteBucketLifecycleRequest deleteBucketLifecycleRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public DeleteBucketMetricsConfigurationResponse deleteBucketMetricsConfiguration(DeleteBucketMetricsConfigurationRequest deleteBucketMetricsConfigurationRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public DeleteBucketOwnershipControlsResponse deleteBucketOwnershipControls(DeleteBucketOwnershipControlsRequest deleteBucketOwnershipControlsRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public DeleteBucketPolicyResponse deleteBucketPolicy(DeleteBucketPolicyRequest deleteBucketPolicyRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public DeleteBucketReplicationResponse deleteBucketReplication(DeleteBucketReplicationRequest deleteBucketReplicationRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public DeleteBucketTaggingResponse deleteBucketTagging(DeleteBucketTaggingRequest deleteBucketTaggingRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public DeleteBucketWebsiteResponse deleteBucketWebsite(DeleteBucketWebsiteRequest deleteBucketWebsiteRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public DeleteObjectTaggingResponse deleteObjectTagging(DeleteObjectTaggingRequest deleteObjectTaggingRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public DeleteObjectsResponse deleteObjects(DeleteObjectsRequest deleteObjectsRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public DeletePublicAccessBlockResponse deletePublicAccessBlock(DeletePublicAccessBlockRequest deletePublicAccessBlockRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public GetBucketAccelerateConfigurationResponse getBucketAccelerateConfiguration(GetBucketAccelerateConfigurationRequest getBucketAccelerateConfigurationRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public GetBucketAclResponse getBucketAcl(GetBucketAclRequest getBucketAclRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public GetBucketAnalyticsConfigurationResponse getBucketAnalyticsConfiguration(GetBucketAnalyticsConfigurationRequest getBucketAnalyticsConfigurationRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public GetBucketCorsResponse getBucketCors(GetBucketCorsRequest getBucketCorsRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public GetBucketEncryptionResponse getBucketEncryption(GetBucketEncryptionRequest getBucketEncryptionRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public GetBucketIntelligentTieringConfigurationResponse getBucketIntelligentTieringConfiguration(GetBucketIntelligentTieringConfigurationRequest getBucketIntelligentTieringConfigurationRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public GetBucketInventoryConfigurationResponse getBucketInventoryConfiguration(GetBucketInventoryConfigurationRequest getBucketInventoryConfigurationRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public GetBucketLifecycleConfigurationResponse getBucketLifecycleConfiguration(GetBucketLifecycleConfigurationRequest getBucketLifecycleConfigurationRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public GetBucketLocationResponse getBucketLocation(GetBucketLocationRequest getBucketLocationRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public GetBucketLoggingResponse getBucketLogging(GetBucketLoggingRequest getBucketLoggingRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public GetBucketMetricsConfigurationResponse getBucketMetricsConfiguration(GetBucketMetricsConfigurationRequest getBucketMetricsConfigurationRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public GetBucketNotificationConfigurationResponse getBucketNotificationConfiguration(GetBucketNotificationConfigurationRequest getBucketNotificationConfigurationRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public GetBucketOwnershipControlsResponse getBucketOwnershipControls(GetBucketOwnershipControlsRequest getBucketOwnershipControlsRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public GetBucketPolicyResponse getBucketPolicy(GetBucketPolicyRequest getBucketPolicyRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public GetBucketPolicyStatusResponse getBucketPolicyStatus(GetBucketPolicyStatusRequest getBucketPolicyStatusRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public GetBucketReplicationResponse getBucketReplication(GetBucketReplicationRequest getBucketReplicationRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public GetBucketRequestPaymentResponse getBucketRequestPayment(GetBucketRequestPaymentRequest getBucketRequestPaymentRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public GetBucketTaggingResponse getBucketTagging(GetBucketTaggingRequest getBucketTaggingRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public GetBucketVersioningResponse getBucketVersioning(GetBucketVersioningRequest getBucketVersioningRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public GetBucketWebsiteResponse getBucketWebsite(GetBucketWebsiteRequest getBucketWebsiteRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public GetObjectAclResponse getObjectAcl(GetObjectAclRequest getObjectAclRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public GetObjectAttributesResponse getObjectAttributes(GetObjectAttributesRequest getObjectAttributesRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public GetObjectLegalHoldResponse getObjectLegalHold(GetObjectLegalHoldRequest getObjectLegalHoldRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public GetObjectLockConfigurationResponse getObjectLockConfiguration(GetObjectLockConfigurationRequest getObjectLockConfigurationRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public GetObjectRetentionResponse getObjectRetention(GetObjectRetentionRequest getObjectRetentionRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public GetObjectTaggingResponse getObjectTagging(GetObjectTaggingRequest getObjectTaggingRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public ResponseInputStream<GetObjectTorrentResponse> getObjectTorrent(GetObjectTorrentRequest getObjectTorrentRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public GetPublicAccessBlockResponse getPublicAccessBlock(GetPublicAccessBlockRequest getPublicAccessBlockRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }

    @Override
    public ListBucketAnalyticsConfigurationsResponse listBucketAnalyticsConfigurations(ListBucketAnalyticsConfigurationsRequest listBucketAnalyticsConfigurationsRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public ListBucketIntelligentTieringConfigurationsResponse listBucketIntelligentTieringConfigurations(ListBucketIntelligentTieringConfigurationsRequest listBucketIntelligentTieringConfigurationsRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public ListBucketInventoryConfigurationsResponse listBucketInventoryConfigurations(ListBucketInventoryConfigurationsRequest listBucketInventoryConfigurationsRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public ListBucketMetricsConfigurationsResponse listBucketMetricsConfigurations(ListBucketMetricsConfigurationsRequest listBucketMetricsConfigurationsRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public ListBucketsResponse listBuckets() {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public ListBucketsResponse listBuckets(ListBucketsRequest listBucketsRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public ListMultipartUploadsResponse listMultipartUploads(ListMultipartUploadsRequest listMultipartUploadsRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public ListObjectVersionsResponse listObjectVersions(ListObjectVersionsRequest listObjectVersionsRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public ListObjectsResponse listObjects(ListObjectsRequest listObjectsRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public ListObjectsV2Iterable listObjectsV2Paginator(ListObjectsV2Request listObjectsV2Request) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public ListPartsResponse listParts(ListPartsRequest listPartsRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public PutBucketAccelerateConfigurationResponse putBucketAccelerateConfiguration(PutBucketAccelerateConfigurationRequest putBucketAccelerateConfigurationRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public PutBucketAclResponse putBucketAcl(PutBucketAclRequest putBucketAclRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public PutBucketAnalyticsConfigurationResponse putBucketAnalyticsConfiguration(PutBucketAnalyticsConfigurationRequest putBucketAnalyticsConfigurationRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public PutBucketCorsResponse putBucketCors(PutBucketCorsRequest putBucketCorsRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public PutBucketEncryptionResponse putBucketEncryption(PutBucketEncryptionRequest putBucketEncryptionRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public PutBucketIntelligentTieringConfigurationResponse putBucketIntelligentTieringConfiguration(PutBucketIntelligentTieringConfigurationRequest putBucketIntelligentTieringConfigurationRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public PutBucketInventoryConfigurationResponse putBucketInventoryConfiguration(PutBucketInventoryConfigurationRequest putBucketInventoryConfigurationRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public PutBucketLifecycleConfigurationResponse putBucketLifecycleConfiguration(PutBucketLifecycleConfigurationRequest putBucketLifecycleConfigurationRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public PutBucketLoggingResponse putBucketLogging(PutBucketLoggingRequest putBucketLoggingRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public PutBucketMetricsConfigurationResponse putBucketMetricsConfiguration(PutBucketMetricsConfigurationRequest putBucketMetricsConfigurationRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public PutBucketNotificationConfigurationResponse putBucketNotificationConfiguration(PutBucketNotificationConfigurationRequest putBucketNotificationConfigurationRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public PutBucketOwnershipControlsResponse putBucketOwnershipControls(PutBucketOwnershipControlsRequest putBucketOwnershipControlsRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public PutBucketPolicyResponse putBucketPolicy(PutBucketPolicyRequest putBucketPolicyRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public PutBucketReplicationResponse putBucketReplication(PutBucketReplicationRequest putBucketReplicationRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public PutBucketRequestPaymentResponse putBucketRequestPayment(PutBucketRequestPaymentRequest putBucketRequestPaymentRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public PutBucketTaggingResponse putBucketTagging(PutBucketTaggingRequest putBucketTaggingRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public PutBucketVersioningResponse putBucketVersioning(PutBucketVersioningRequest putBucketVersioningRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public PutBucketWebsiteResponse putBucketWebsite(PutBucketWebsiteRequest putBucketWebsiteRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public PutObjectAclResponse putObjectAcl(PutObjectAclRequest putObjectAclRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public PutObjectLegalHoldResponse putObjectLegalHold(PutObjectLegalHoldRequest putObjectLegalHoldRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public PutObjectLockConfigurationResponse putObjectLockConfiguration(PutObjectLockConfigurationRequest putObjectLockConfigurationRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public PutObjectRetentionResponse putObjectRetention(PutObjectRetentionRequest putObjectRetentionRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public PutObjectTaggingResponse putObjectTagging(PutObjectTaggingRequest putObjectTaggingRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public PutPublicAccessBlockResponse putPublicAccessBlock(PutPublicAccessBlockRequest putPublicAccessBlockRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public RestoreObjectResponse restoreObject(RestoreObjectRequest restoreObjectRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }

    @Override
    public UploadPartResponse uploadPart(UploadPartRequest uploadPartRequest, RequestBody requestBody) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public UploadPartCopyResponse uploadPartCopy(UploadPartCopyRequest uploadPartCopyRequest) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public WriteGetObjectResponseResponse writeGetObjectResponse(WriteGetObjectResponseRequest writeGetObjectResponseRequest, RequestBody requestBody) {
        throw new UnsupportedOperationException("Not implemented in test client");
    }
    
    @Override
    public S3ServiceClientConfiguration serviceClientConfiguration() {
        throw new UnsupportedOperationException("Not implemented in test client");
    }

}
