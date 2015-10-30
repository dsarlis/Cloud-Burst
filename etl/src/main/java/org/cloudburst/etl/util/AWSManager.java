package org.cloudburst.etl.util;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.model.*;
import org.cloudburst.etl.model.Tweet;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to manage communications with Amazon.
 */
public class AWSManager {

	private static final Logger logger = LoggerFactory.getLogger(AWSManager.class);
	private static final int TIMEOUT = 1800000;

	private AmazonS3Client s3Client;

	public AWSManager() {
		String accessKey = System.getenv().get("CLOUD_BURST_ACCESS_KEY");
		String secretKey = System.getenv().get("CLOUD_BURST_SECRET_KEY");
		ClientConfiguration config = new ClientConfiguration();
		config.setConnectionTimeout(TIMEOUT);
		config.setSocketTimeout(TIMEOUT);

		BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(accessKey, secretKey);

		s3Client = new AmazonS3Client(basicAWSCredentials, config);
	}

	/**
	 * List all files in the bucket with the given prefix.
	 */
	public List<String> listFiles(String bucketName, String prefix) {
		logger.info("Listing files with bucketName={} and prefix={}", bucketName, prefix);
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

		logger.info("Done listing files with bucketName={} and prefix={}", bucketName, prefix);
		return files;
	}

	/**
	 * Get input stream from the given object with the given key in bucket.
	 */
	public InputStream getObjectInputStream(String bucketName, String key) {
		logger.info("Getting object input stream bucketName={} and key={}", bucketName, key);
		S3Object s3object = s3Client.getObject(new GetObjectRequest(bucketName, key));

		logger.info("Done getting object input stream bucketName={} and key={}", bucketName, key);
		return s3object.getObjectContent();
	}

	/**
	 * Stores file in a bucket with the given key.
	 */
	public void storeInBucket(String finalName, String bucketName, String key) {
		logger.info("Storing fileName={} in bucketName={} and key={}", finalName, bucketName, key);
		File file = new File(finalName);

		s3Client.putObject(new PutObjectRequest(bucketName, key, file));
		logger.info("Dne storing fileName={} in bucketName={} and key={}", finalName, bucketName, key);
	}

}
