package org.nmq;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.nmq.channelhandler.ClientMessageHandler;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClientHandlerManager {

    private final ConcurrentMap<String, TopicHandlerGroup> handlerMap = new ConcurrentHashMap<>();

    public ClientHandlerManager(Set<String> topics) {
        for (String topic : topics) {
            this.handlerMap.put(topic, new TopicHandlerGroup(topic));
        }
    }

    public boolean addHandler(String topic, ClientMessageHandler handler) {
        return handlerMap.get(topic).add(handler);
    }

    public boolean removeHandler(String topic, ClientMessageHandler handler) {
        return handlerMap.get(topic).remove(handler);
    }

    public Set<String> getTopics() {
        return new HashSet<String>(handlerMap.keySet());
    }

    public TopicHandlerGroup getHandlerGroup(String topic) {
        return handlerMap.get(topic);
    }

    public int getConnectionCount(String topic) {
        return handlerMap.get(topic).size();
    }

    public int getAllConnectionCount() {
        int connectionCount = 0;
        for (TopicHandlerGroup handlerGroup : handlerMap.values()) {
            connectionCount += handlerGroup.size();
        }
        return connectionCount;
    }

}
