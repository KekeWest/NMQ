package org.nmq;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.netty.channel.Channel;
import io.netty.util.concurrent.GlobalEventExecutor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClientChannelManager {

    private final ConcurrentMap<String, TopicChannelGroup> channelMap = new ConcurrentHashMap<>();

    public ClientChannelManager(Set<String> topics) {
        for (String topic : topics) {
            this.channelMap.put(topic, new TopicChannelGroup(topic, GlobalEventExecutor.INSTANCE));
        }
    }

    public boolean addChannel(String topic, Channel channel) {
        return channelMap.get(topic).add(channel);
    }

    public Set<String> getTopics() {
        return new HashSet<String>(channelMap.keySet());
    }

    public int getConnectionCount(String topic) {
        return channelMap.get(topic).size();
    }

    public int getAllConnectionCount() {
        int connectionCount = 0;
        for (TopicChannelGroup channelGroup : channelMap.values()) {
            connectionCount += channelGroup.size();
        }
        return connectionCount;
    }

}
