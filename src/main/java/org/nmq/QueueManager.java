package org.nmq;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import io.netty.buffer.ByteBuf;

public class QueueManager {

    protected final ConcurrentHashMap<String, LinkedBlockingQueue<ByteBuf>> queueMap = new ConcurrentHashMap<>();

    public QueueManager(Set<String> topics) {
        this(topics, null);
    }

    public QueueManager(Set<String> topics, Integer maxLength) {
        for (String topic : topics) {
            if (maxLength == null) {
                this.queueMap.put(topic, new LinkedBlockingQueue<ByteBuf>());
            } else {
                this.queueMap.put(topic, new LinkedBlockingQueue<ByteBuf>(maxLength));
            }
        }
    }

    public boolean offer(String topic, ByteBuf bytes) {
        boolean result = getQueue(topic).offer(bytes);
        return result;
    }

    public ByteBuf take(String topic) throws InterruptedException {
        return getQueue(topic).take();
    }

    protected LinkedBlockingQueue<ByteBuf> getQueue(String topic) {
        return this.queueMap.get(topic);
    }

}
