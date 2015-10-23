package org.cloudburst.etl.util;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class AWSManager {

    private AmazonS3Client s3Client;

    public AWSManager() {
        String accessKey = System.getenv().get("CLOUD_BURST_ACCESS_KEY");
        String secretKey = System.getenv().get("CLOUD_BURST_SECRET_KEY");
        BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(accessKey, secretKey);

        s3Client = new AmazonS3Client(basicAWSCredentials);
    }


    public List<String> listFiles(String bucketName, String prefix) {
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName);
        if (prefix != null) {
            listObjectsRequest = listObjectsRequest.withPrefix(prefix);
        }

        ObjectListing objectListing = null;
        List<String> files = new ArrayList<String>();

        do {
            objectListing = s3Client.listObjects(listObjectsRequest);
            for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                files.add(objectSummary.getKey());
            }
            listObjectsRequest.setMarker(objectListing.getNextMarker());
        } while (objectListing.isTruncated());

        return files;
    }

    public InputStream getObjectInputStream(String bucketName, String key) {
        S3Object s3object = s3Client.getObject(new GetObjectRequest(bucketName, key));

        return s3object.getObjectContent();
    }

}
