package org.nmq.receiver;

import java.util.concurrent.BlockingQueue;

import org.nmq.Message;

import lombok.Setter;

public abstract class MessageReceiver implements Runnable {

    @Setter
    protected BlockingQueue<Message> queue;

    public MessageReceiver() {

    }

    @Override
    public void run() {
        while(true) {
            try {
                Message msg = queue.take();
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
