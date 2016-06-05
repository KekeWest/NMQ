package org.nmq.receiver;

import org.nmq.Message;
import org.nmq.QueueManager;

import lombok.Setter;

public abstract class MessageReceiver implements Runnable {

    @Setter
    private String topic;

    @Setter
    private QueueManager queueManager;

    public MessageReceiver() {

    }

    @Override
    public void run() {
        while(true) {
            try {
                Message msg = queueManager.take(topic);
                if (msg.getBytes() == null) {
                    return;
                }
                receivedMessage(msg.getBytes());
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    public abstract void receivedMessage(byte[] bytes);
}
