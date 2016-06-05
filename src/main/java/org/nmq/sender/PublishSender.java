package org.nmq.sender;

import org.nmq.ClientChannelManager;
import org.nmq.Message;
import org.nmq.QueueManager;

import io.netty.channel.group.ChannelGroupFuture;

public class PublishSender extends MessageSender {

    public PublishSender(String topic, ClientChannelManager channelManager, QueueManager queueManager) {
        super(topic, channelManager, queueManager);
    }

    protected ChannelGroupFuture send(Message msg) {
        return channelManager.write(topic, msg);
    }

}
