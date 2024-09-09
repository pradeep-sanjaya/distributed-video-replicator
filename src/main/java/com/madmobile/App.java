package com.madmobile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) throws Exception {
        logger.info("Starting the application...");

        // Initialize ClusterManager and start the cluster
        ClusterManager clusterManager = new ClusterManager();

        // Initialize MessageReceiver and register it as a listener for master change events
        MessageReceiver messageReceiver = new MessageReceiver();
        clusterManager.addClusterChangeListener(messageReceiver);

        clusterManager.start();

        // Keep the application running
        logger.info("Application running, waiting for cluster events...");
        while (true) {
            Thread.sleep(1000); // Prevent the application from exiting
        }
    }
}