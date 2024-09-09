package com.madmobile;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.net.URL;

public class MasterFileManager {
    private static final Logger logger = LoggerFactory.getLogger(MasterFileManager.class);
    private AmazonS3 s3Client;
    private String lastFileHash = null;
    private PropertiesLoader propertiesLoader;

    public MasterFileManager() {
        propertiesLoader = new PropertiesLoader();
        initializeS3Client();
    }

    private void initializeS3Client() {
        String region = propertiesLoader.getProperty("s3.region");

        s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(region)
                .withCredentials(new ProfileCredentialsProvider())
                .build();
    }

    public void handleM3u8File(String s3Url) {
        try {
            // Load the bucketName and key from properties
            String bucketName = propertiesLoader.getProperty("s3.bucketName");

            // Extract the filename from the URL
            String filename = extractFilenameFromUrl(s3Url);

            logger.info("Handling m3u8 file from S3 URL: {}, filename: {}", s3Url, filename);

            // Download the file and save it with the same filename
            File downloadedFile = new File("./" + filename);
            s3Client.getObject(new GetObjectRequest(bucketName, filename), downloadedFile);

            // Calculate file hash
            String newFileHash = getFileHash(downloadedFile.getPath());

            // Check if the file is new or updated
            if (lastFileHash == null || !newFileHash.equals(lastFileHash)) {
                logger.info("New m3u8 file detected, updating...");
                lastFileHash = newFileHash;

                // Trigger replication of the file to all nodes
                ClusterManager.getInstance().replicateFile(downloadedFile.getPath());
            } else {
                logger.info("No changes detected in the m3u8 file.");
            }

        } catch (AmazonS3Exception e) {
            if (e.getErrorCode().equals("ExpiredToken")) {
                logger.error("AWS token expired, attempting to refresh credentials and retry operation.");
                refreshCredentials();
                retryS3Operation(s3Url);
            } else {
                logger.error("S3 Error: {}", e.getMessage(), e);
            }
        } catch (IOException | NoSuchAlgorithmException e) {
            logger.error("Error processing m3u8 file: {}", e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Unexpected error: {}", e.getMessage(), e);
        }
    }

    private void refreshCredentials() {
        logger.info("Refreshing AWS credentials...");
        initializeS3Client(); // Re-initialize the S3 client with fresh credentials
    }

    private void retryS3Operation(String s3Url) {
        try {
            // Retry the operation after refreshing credentials
            handleM3u8File(s3Url);
        } catch (Exception e) {
            logger.error("Error retrying S3 operation after refreshing credentials: {}", e.getMessage(), e);
        }
    }

    private String extractFilenameFromUrl(String s3Url) {
        try {
            URL url = new URL(s3Url);
            String filePath = url.getPath();
            return filePath.substring(filePath.lastIndexOf('/') + 1);  // Get the part after the last '/'
        } catch (Exception e) {
            logger.error("Failed to extract filename from URL: {}", s3Url);
            throw new RuntimeException("Invalid URL format", e);
        }
    }

    private String getFileHash(String filePath) throws NoSuchAlgorithmException, IOException {
        logger.debug("Calculating file hash for {}", filePath);
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hash = digest.digest(Files.readAllBytes(Paths.get(filePath)));
        StringBuilder hexString = new StringBuilder();
        for (byte b : hash) {
            hexString.append(String.format("%02x", b));
        }
        return hexString.toString();
    }

    public byte[] readFileToBytes(String filePath) {
        try {
            return Files.readAllBytes(Paths.get(filePath));
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
}