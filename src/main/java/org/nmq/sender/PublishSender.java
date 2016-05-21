package org.nmq.sender;

import org.nmq.ClientChannelManager;
import org.nmq.Message;
import org.nmq.QueueManager;

public class PublishSender extends MessageSender {

    public PublishSender(String topic, ClientChannelManager channelManager, QueueManager queueManager) {
        super(topic, channelManager, queueManager);
    }

    protected void send(Message msg) {
        channelManager.writeAndFlush(topic, msg);
    }

}
