package org.nmq.sender;

import org.nmq.ClientChannelManager;
import org.nmq.Message;
import org.nmq.QueueManager;

public abstract class MessageSender implements Runnable {

    protected final String topic;
    protected final ClientChannelManager channelManager;
    protected final QueueManager queueManager;

    public MessageSender(String topic, ClientChannelManager channelManager, QueueManager queueManager) {
        this.topic = topic;
        this.channelManager = channelManager;
        this.queueManager = queueManager;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Message msg = queueManager.take(topic);
                if (msg.getBytes() == null) {
                    return;
                }
                send(msg);
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
        }
    }

    protected abstract void send(Message msg);

}
