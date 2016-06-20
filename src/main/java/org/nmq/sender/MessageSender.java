package org.nmq.sender;

import java.util.concurrent.BlockingQueue;

import org.nmq.Message;
import org.nmq.TopicHandlerGroup;

public abstract class MessageSender implements Runnable {

    protected final TopicHandlerGroup handlerGroup;
    protected final BlockingQueue<Message> queue;

    public MessageSender(TopicHandlerGroup handlerGroup, BlockingQueue<Message> queue) {
        this.handlerGroup = handlerGroup;
        this.queue = queue;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Message msg = queue.take();
                if (msg.getBytes() == null) {
                    return;
                }
                send(msg);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    protected abstract void send(Message msg);

}
