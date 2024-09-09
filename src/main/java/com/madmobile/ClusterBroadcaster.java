package com.madmobile;

import org.jgroups.JChannel;
import org.jgroups.Message;

public class ClusterBroadcaster {

    public void broadcastMessage(JChannel channel, byte[] payload) throws Exception {
        // Create a message with 'null' destination for broadcasting
        Message msg = new Message(null, payload);

        // Send the message to all nodes in the cluster
        channel.send(msg);

        System.out.println("Broadcasted message to all nodes.");
    }
}
