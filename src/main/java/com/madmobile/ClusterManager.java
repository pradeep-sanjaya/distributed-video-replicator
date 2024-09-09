package com.madmobile;

import org.jgroups.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class ClusterManager implements Receiver {
    private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);
    private JChannel channel;
    private boolean isMaster;
    private List<ClusterChangeListener> listeners = new ArrayList<>();
    private static ClusterManager instance;

    private MasterFileManager masterFileManager;

    public ClusterManager() {
        masterFileManager = new MasterFileManager();
        instance = this; // Singleton pattern
    }

    public static ClusterManager getInstance() {
        return instance;
    }

    public void start() throws Exception {
        // Create a JChannel for the JGroups cluster communication
        channel = new JChannel();
        channel.setReceiver(this);

        // Connect to a named cluster (e.g., "madmobile-cluster")
        String clusterName = "madmobile-cluster";
        logger.info("Connecting to cluster: " + clusterName);
        channel.connect(clusterName);
    }

    @Override
    public void receive(Message msg) {
        System.out.println("Received a message from " + msg.getSrc());

        try {
            if (!isMaster()) {
                byte[] receivedBytes = msg.getRawBuffer();  // Get the byte array payload
                if (receivedBytes != null) {
                    String message = new String(receivedBytes);  // If it's text, convert it back to String
                    System.out.println("Received message: " + message);

                    try {
                        String outputFile = "./replicated_file.m3u8";  // Change the file name as needed
                        saveReceivedFile(receivedBytes, outputFile);

                        logger.info("File saved successfully at {}", outputFile);
                    } catch (Exception e) {
                        logger.error("Error processing received file: {}", e.getMessage(), e);
                    }
                } else {
                    System.out.println("No byte array found in message.");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void replicateFile(String filePath) {
        logger.info("Starting file replication for: {}", filePath);
        try {

            byte[] payload = masterFileManager.readFileToBytes(filePath);
            logger.info(Arrays.toString(payload));
            // Create a broadcaster instance
            ClusterBroadcaster broadcaster = new ClusterBroadcaster();

            // Broadcast message to all nodes
            broadcaster.broadcastMessage(channel, payload);

            logger.info("File byte stream sent to the cluster for replication.");

        } catch (IOException e) {
            logger.error("Error reading file for replication: {}", e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Error sending file to cluster: {}", e.getMessage(), e);
        }
    }



    private void saveReceivedFile(byte[] fileBytes, String outputFilePath) {
        try {
            // Write the decompressed byte array to the specified file path
            Files.write(Paths.get(outputFilePath), fileBytes);
            logger.info("File saved successfully at {}", outputFilePath);
        } catch (IOException e) {
            logger.error("Error saving received file: {}", e.getMessage(), e);
        }
    }

    private byte[] compress(byte[] data) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream)) {
            gzipOutputStream.write(data);
        }
        return byteArrayOutputStream.toByteArray();
    }

    private byte[] decompress(byte[] compressedBytes) throws IOException {
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(compressedBytes);
        try (GZIPInputStream gzipInputStream = new GZIPInputStream(byteArrayInputStream);
             ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {

            byte[] buffer = new byte[1024];
            int len;
            while ((len = gzipInputStream.read(buffer)) != -1) {
                byteArrayOutputStream.write(buffer, 0, len);
            }
            return byteArrayOutputStream.toByteArray();
        }
    }

    @Override
    public void viewAccepted(View view) {
        boolean previousMaster = isMaster;

        if (view.getMembers().get(0).equals(channel.getAddress())) {
            isMaster = true;
            logger.info(">>>>> I am the master.");
        } else {
            isMaster = false;
            logger.info(">>>>> I am a slave.");
        }

        // Notify listeners if the master status changes
        if (isMaster != previousMaster) {
            logger.info("Notifying listeners about master change.");
            notifyListeners(isMaster);  // Notify MessageReceiver that master status has changed
        }
    }

    public void addClusterChangeListener(ClusterChangeListener listener) {
        listeners.add(listener);
    }

    private void notifyListeners(boolean isMaster) {
        for (ClusterChangeListener listener : listeners) {
            listener.onMasterChange(isMaster);
        }
    }

    public boolean isMaster() {
        return isMaster;
    }

    public void close() {
        if (channel != null) {
            channel.close();
        }
    }
}