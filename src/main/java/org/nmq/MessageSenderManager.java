package org.nmq;

import java.util.ArrayList;
import java.util.List;

import org.nmq.enums.ChannelType;
import org.nmq.sender.PublishSender;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MessageSenderManager {

    private final ChannelType channelType;
    private final ClientHandlerManager handlerManager;
    private final QueueManager queueManager;
    private final List<Thread> senderThreads = new ArrayList<>();

    @Getter
    private boolean started = false;

    public MessageSenderManager(ChannelType channelType,
        ClientHandlerManager handlerManager, QueueManager queueManager) {
        this.channelType = channelType;
        this.handlerManager = handlerManager;
        this.queueManager = queueManager;
    }

    public void start() {
        if (isStarted()) {
            throw new IllegalStateException("MessageSenderManager has already started");
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
            PublishSender sender = new PublishSender(handlerManager.getHandlerGroup(topic),
                queueManager.getQueue(topic));

            Thread senderThread = new Thread(sender);
            senderThread.setName("PublishSender topic: " + topic);
            senderThread.start();
            senderThreads.add(senderThread);
        }
    }

    public void shutdown(boolean now) throws InterruptedException {
        if (!isStarted()) {
            throw new IllegalStateException("MessageSenderManager does not start");
        }

        if (now) {
            queueManager.clearAll();
        }

        queueManager.setShutdownMessage();
        for (Thread senderThread : senderThreads) {
            senderThread.join();
        }
        senderThreads.clear();

        started = false;
    }

}
