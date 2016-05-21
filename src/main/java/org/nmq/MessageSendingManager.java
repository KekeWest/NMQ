package org.nmq;

import org.nmq.enums.ChannelType;
import org.nmq.sender.PublishSender;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MessageSendingManager {

    private final ChannelType channelType;
    private final ClientChannelManager channelManager;
    private final QueueManager queueManager;

    @Getter
    private boolean started = false;

    public MessageSendingManager(ChannelType channelType,
        ClientChannelManager channelManager, QueueManager queueManager) {
        this.channelType = channelType;
        this.channelManager = channelManager;
        this.queueManager = queueManager;
    }

    public void start() {
        if (isStarted()) {
            throw new IllegalStateException("MessageSendingManager has already started");
        }

        switch (channelType) {
        case PUB:
            startPublishSender();
            break;
        case PUSH:

            break;
        default:
            throw new IllegalStateException("Unsupported channel type: " + channelType.name());
        }
        started = true;
    }

    private void startPublishSender() {
        for (String topic : queueManager.getTopics()) {
            PublishSender sender = new PublishSender(topic, channelManager, queueManager);
            Thread senderThread = new Thread(sender);
            senderThread.setName("PublishSender topic: " + topic);
            senderThread.start();
        }
    }

    public void shutdown(boolean now) throws InterruptedException {
        if (!isStarted()) {
            throw new IllegalStateException("MessageSendingManager does not start");
        }

        if (now) {
            queueManager.clearAll();
        }
        queueManager.offerAll(null);

        started = false;
    }

}
