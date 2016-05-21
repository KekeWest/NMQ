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
        boolean result = queueMap.get(topic).offer(new Message(topic, bytes));
        return result;
    }

    public boolean offerAll(byte[] bytes) {
        boolean result = true;

        for (String topic : getTopics()) {
            boolean r = offer(topic, bytes);
            if (!r) {
                result = false;
            }
        }

        return result;
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
