package org.nmq;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.nmq.receiver.MessageReceiver;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MessageReceiverManager {

    private final QueueManager queueManager;
    private final Map<String, MessageReceiver> receivers;
    private final List<Thread> receiverThreads = new ArrayList<>();

    @Getter
    private boolean started = false;

    public MessageReceiverManager(Map<String, MessageReceiver> receivers, QueueManager queueManager) {
        this.queueManager = queueManager;
        this.receivers = receivers;
    }

    public void start() {
        if (isStarted()) {
            throw new IllegalStateException("MessageReceiverManager has already started");
        }

        for (Entry<String, MessageReceiver> entry : receivers.entrySet()) {
            MessageReceiver receiver = entry.getValue();
            String topic = entry.getKey();
            receiver.setTopic(topic);
            receiver.setQueueManager(queueManager);
            Thread receiverThread = new Thread(receiver);
            receiverThread.setName(receiver.getClass().getSimpleName() + " topic: " + topic);
            receiverThread.start();
            receiverThreads.add(receiverThread);
        }

        started = true;
    }

    public void shutdown(boolean now) throws InterruptedException {
        if (!isStarted()) {
            throw new IllegalStateException("MessageReceiverManager does not start");
        }

        if (now) {
            queueManager.clearAll();
        }

        queueManager.setShutdownMessage();
        for (Thread receiverThread : receiverThreads) {
            receiverThread.join();
        }
        receiverThreads.clear();

        started = false;
    }

}
