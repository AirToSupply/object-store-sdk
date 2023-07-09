package tech.odes.object.storage.s3.api;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.*;
import com.amazonaws.util.StringUtils;
import tech.odes.object.storage.s3.XferMgrProgress;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class S3Client implements Closeable {

    private static Log LOG = LogFactory.getLog(S3Client.class);

    private static final String FILE_SEPARATOR = "/";

    private final String bucket;

    private final AmazonS3 amazonS3;

    private S3Client(Builder builder) {
        this.bucket = builder.bucket;
        this.amazonS3 = AmazonS3ClientBuilder
            .standard()
            .withPathStyleAccessEnabled(true)
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(
                    builder.endpoint,
                    Regions.fromName(StringUtils.isNullOrEmpty(builder.region) ? Regions.DEFAULT_REGION.getName() : builder.region).getName()))
            .withCredentials(
                new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(
                        builder.accessKey,
                        builder.secretKey)))
            .build();
    }

    public boolean ed(String path) {
        return this.amazonS3.doesObjectExist(this.bucket, path.endsWith(FILE_SEPARATOR) ? path : path + FILE_SEPARATOR);
    }

    public boolean ef(String path) {
        return this.amazonS3.doesObjectExist(this.bucket, path);
    }

    public ObjectListing du() {
        return this.amazonS3.listObjects(this.bucket);
    }

    public ObjectListing du(String path) {
        return this.amazonS3.listObjects(this.bucket, path);
    }

    public void mkdir(String path) {
        String objectName = path.endsWith(FILE_SEPARATOR) ? path : path + FILE_SEPARATOR;
        if (!this.amazonS3.doesObjectExist(this.bucket, objectName)) {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(0);
            InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
            PutObjectRequest putObjectRequest = new PutObjectRequest(this.bucket, objectName, emptyContent, metadata);
            this.amazonS3.putObject(putObjectRequest);
        } else {
            new RuntimeException("Object [" + path + "] has been already existed!");
        }
    }

    public void mkdir(List<String> paths) {
        paths.stream().forEach(p -> mkdir(p));
    }

    public void rm(String path) {
        String objectName = path;
        if (!this.amazonS3.doesObjectExist(this.bucket, objectName)) {
            new RuntimeException("Object [" + path + "] is not existed!");
        } else {
            DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(this.bucket, objectName);
            this.amazonS3.deleteObject(deleteObjectRequest);
        }
    }

    public void rm(List<String> paths) {
        paths.stream().forEach(p -> rm(p));
    }

    public void rmdir(String path) {
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(this.bucket)
                .withPrefix(path);

        ObjectListing objectListing = this.amazonS3.listObjects(listObjectsRequest);

        while (true) {
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                this.amazonS3.deleteObject(this.bucket, objectSummary.getKey());
            }
            if (objectListing.isTruncated()) {
                objectListing = this.amazonS3.listNextBatchOfObjects(objectListing);
            } else {
                break;
            }
        }
    }

    public void rmdir(List<String> paths) {
        paths.stream().forEach(p -> rmdir(p));
    }

    public List<S3ObjectSummary> ls(String path) {
        String objectName = path.endsWith(FILE_SEPARATOR) ? path : path + FILE_SEPARATOR;
        ListObjectsV2Request listObjectsV2Request = new ListObjectsV2Request()
                .withBucketName(this.bucket)
                .withPrefix(objectName)
                .withDelimiter(FILE_SEPARATOR);
        ListObjectsV2Result result = this.amazonS3.listObjectsV2(listObjectsV2Request);
        List<S3ObjectSummary> file = result.getObjectSummaries();
        List<S3ObjectSummary> dir = result.getCommonPrefixes().stream().map(s -> {
            S3ObjectSummary summary = new S3ObjectSummary();
            summary.setBucketName(result.getBucketName());
            summary.setKey(s);
            return summary;
        }).collect(Collectors.toList());
        dir.addAll(file);
        return dir;
    }

    public void cp(String src, String dest) {
        CopyObjectRequest copyObjectRequest = new CopyObjectRequest()
                .withSourceBucketName(this.bucket)
                .withSourceKey(src)
                .withDestinationBucketName(this.bucket)
                .withDestinationKey(dest);
        this.amazonS3.copyObject(copyObjectRequest);
    }

    public void cp(List<String> src, String dest) {
        src.stream().forEach(s -> cp(s, dest));
    }

    public void mv(String src, String dest) {
        cp(src, dest);
        rm(src);
    }

    public void mv(Map<String, String> srcToDest) {
        srcToDest.forEach((src, dest) -> mv(src, dest));
    }

    public void rz(String path, File file) {
        TransferManager xfer_mgr = TransferManagerBuilder.standard().withS3Client(this.amazonS3).build();
        try {
            String objectName = (path.endsWith(FILE_SEPARATOR) ? path : path + FILE_SEPARATOR) + file.getName();
            Upload xfer = xfer_mgr.upload(this.bucket, objectName, file);
            // loop with Transfer.isDone()
            XferMgrProgress.showTransferProgress(xfer);
            //  or block with Transfer.waitForCompletion()
            XferMgrProgress.waitForCompletion(xfer);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            LOG.error("[rz ERROR] " + e.getErrorMessage());
        }
        xfer_mgr.shutdownNow();
    }

    public void rz(String path, List<File> files) {
        TransferManager xfer_mgr = TransferManagerBuilder.standard().withS3Client(this.amazonS3).build();
        try {
            files.stream().forEach(f -> {
                String objectName = (path.endsWith(FILE_SEPARATOR) ? path : path + FILE_SEPARATOR) + f.getName();
                Upload xfer = xfer_mgr.upload(this.bucket, objectName, f);
                // loop with Transfer.isDone()
                XferMgrProgress.showTransferProgress(xfer);
                //  or block with Transfer.waitForCompletion()
                XferMgrProgress.waitForCompletion(xfer);
            });
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            LOG.error("[rz ERROR] " + e.getErrorMessage());
        }
        xfer_mgr.shutdownNow();
    }

    public void rzdir(String path, File dir, boolean includeSubdirectories) {
        TransferManager xfer_mgr = TransferManagerBuilder.standard().withS3Client(this.amazonS3).build();
        try {
            MultipleFileUpload xfer = xfer_mgr.uploadDirectory(this.bucket, path, dir, includeSubdirectories);
            // loop with Transfer.isDone()
            XferMgrProgress.showTransferProgress(xfer);
            // or block with Transfer.waitForCompletion()
            XferMgrProgress.waitForCompletion(xfer);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            LOG.error("[rzdir ERROR] " + e.getErrorMessage());
        }
        xfer_mgr.shutdownNow();
    }

    public void rzdir(String path, File dir) {
        this.rzdir(path, dir, true);
    }

    public void sz(String src, File dest) {
        String file = Path.of(src).getFileName().toString();
        File target = new File(dest.getAbsolutePath(), file);
        TransferManager xfer_mgr = TransferManagerBuilder.standard().withS3Client(this.amazonS3).build();
        try {
            Download xfer = xfer_mgr.download(this.bucket, src, target);
            // loop with Transfer.isDone()
            XferMgrProgress.showTransferProgress(xfer);
            // or block with Transfer.waitForCompletion()
            XferMgrProgress.waitForCompletion(xfer);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            LOG.error("[sz ERROR] " + e.getErrorMessage());
        }
        xfer_mgr.shutdownNow();
    }

    public void sz(List<String> srcs, File dest) {
        TransferManager xfer_mgr = TransferManagerBuilder.standard().withS3Client(this.amazonS3).build();
        try {
            srcs.stream().forEach(src -> {
                String file = Path.of(src).getFileName().toString();
                File target = new File(dest.getAbsolutePath(), file);
                Download xfer = xfer_mgr.download(this.bucket, src, target);
                // loop with Transfer.isDone()
                XferMgrProgress.showTransferProgress(xfer);
                // or block with Transfer.waitForCompletion()
                XferMgrProgress.waitForCompletion(xfer);
            });
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            LOG.error("[sz ERROR] " + e.getErrorMessage());
        }
        xfer_mgr.shutdownNow();
    }

    public void szdir(String path, File dest) {
        TransferManager xfer_mgr = TransferManagerBuilder.standard().withS3Client(this.amazonS3).build();
        try {
            MultipleFileDownload xfer = xfer_mgr.downloadDirectory(this.bucket, path, dest);
            // loop with Transfer.isDone()
            XferMgrProgress.showTransferProgress(xfer);
            // or block with Transfer.waitForCompletion()
            XferMgrProgress.waitForCompletion(xfer);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            LOG.error("[szdir ERROR] " + e.getErrorMessage());
        }
        xfer_mgr.shutdownNow();
    }

    @Override
    public void close() {
        this.amazonS3.shutdown();
    }

    public static class Builder {
        private String endpoint;

        private String region;

        private String accessKey;

        private String secretKey;

        private String bucket;

        public Builder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder region(String region) {
            this.region = region;
            return this;
        }

        public Builder accessKey(String accessKey) {
            this.accessKey = accessKey;
            return this;
        }

        public Builder secretKey(String secretKey) {
            this.secretKey = secretKey;
            return this;
        }

        public Builder bucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public S3Client build() {
            return new S3Client(this);
        }
    }
}
