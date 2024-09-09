package com.madmobile;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageReceiver implements ClusterChangeListener {

    private static final Logger logger = LoggerFactory.getLogger(MessageReceiver.class);
    private PropertiesLoader propertiesLoader;
    private boolean isMaster = false;
    private boolean listening = false;
    private Connection connection;
    private Channel channel;

    public MessageReceiver() {
        propertiesLoader = new PropertiesLoader();
    }

    @Override
    public void onMasterChange(boolean isMaster) {
        logger.info("onMasterChange called. isMaster: {}", isMaster);
        this.isMaster = isMaster;

        if (isMaster && !listening) {
            logger.info("Now master. Starting to listen for RabbitMQ messages.");
            try {
                startListening();
            } catch (Exception e) {
                logger.error("Failed to start listening for messages", e);
            }
        } else if (!isMaster && listening) {
            logger.info("No longer master. Stopping listening for RabbitMQ messages.");
            stopListening();
        }
    }

    public void startListening() throws Exception {
        listening = true;

        // Create a connection factory for RabbitMQ
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(propertiesLoader.getProperty("rabbitmq.host"));
        factory.setPort(propertiesLoader.getIntProperty("rabbitmq.port"));
        factory.setUsername(propertiesLoader.getProperty("rabbitmq.username"));
        factory.setPassword(propertiesLoader.getProperty("rabbitmq.password"));

        logger.info("Connecting to RabbitMQ...");

        // Open a connection and a channel to RabbitMQ
        connection = factory.newConnection();
        channel = connection.createChannel();
        logger.info("RabbitMQ connection established.");

        // Load the queue name from properties and declare the queue
        String queueName = propertiesLoader.getProperty("rabbitmq.queueName");
        logger.info("Declaring queue: {}", queueName);

        channel.queueDeclare(queueName, true, false, false, null);
        logger.info("Queue declared and listening for messages on queue: " + queueName);

        // Start consuming messages
        channel.basicConsume(queueName, false, (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            logger.info("Received message: {}", message);

            if (isMaster) {
                logger.info("Processing message as master");
                MasterFileManager masterFileManager = new MasterFileManager();
                masterFileManager.handleM3u8File(message);
            } else {
                logger.info("Ignoring message since not master");
            }

            // Acknowledge the message after processing
            logger.info("Acknowledging message");
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        }, consumerTag -> logger.info("Consumer canceled: {}", consumerTag));
    }

    public void stopListening() {
        if (!listening) {
            logger.warn("Not currently listening for messages.");
            return;
        }

        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
                logger.info("RabbitMQ channel closed.");
            }
            if (connection != null && connection.isOpen()) {
                connection.close();
                logger.info("RabbitMQ connection closed.");
            }
        } catch (Exception e) {
            logger.error("Error closing RabbitMQ connection or channel", e);
        } finally {
            listening = false;
            logger.info("Stopped listening for messages.");
        }
    }
}