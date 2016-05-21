package org.nmq;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueueManager {

    private final ConcurrentMap<String, BlockingQueue<Message>> queueMap = new ConcurrentHashMap<>();

    public QueueManager(Set<String> topics) {
        this(topics, null);
    }

    public QueueManager(Set<String> topics, Integer maxLength) {
        super();
        for (String topic : topics) {
            if (maxLength == null) {
                this.queueMap.put(topic, new LinkedBlockingQueue<Message>());
            } else {
                this.queueMap.put(topic, new LinkedBlockingQueue<Message>(maxLength));
            }
        }
    }

    public boolean offer(String topic, byte[] bytes) {
        return offer(topic, new Message(topic, bytes));
    }

    public boolean offer(String topic, Message msg) {
        return queueMap.get(topic).offer(msg);
    }

    public boolean offerAll(byte[] bytes) {
        return offerAll(new Message(null, bytes));
    }

    private boolean offerAll(Message msg) {
        boolean result = true;

        for (String topic : getTopics()) {
            boolean r = offer(topic, msg);
            if (!r) {
                result = false;
            }
        }

        return result;
    }

    public byte[] poll(String topic) {
        Message msg = queueMap.get(topic).poll();
        if (msg == null) {
            return null;
        }
        return msg.getBytes();
    }

    public Message take(String topic) throws InterruptedException {
        return queueMap.get(topic).take();
    }

    public void clear(String topic) {
        queueMap.get(topic).clear();
    }

    public void clearAll() {
        for (String topic : getTopics()) {
            clear(topic);
        }
    }

    public Set<String> getTopics() {
        return new HashSet<String>(queueMap.keySet());
    }

}
