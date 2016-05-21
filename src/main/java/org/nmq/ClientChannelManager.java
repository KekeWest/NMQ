package org.nmq;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.netty.channel.Channel;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.util.concurrent.GlobalEventExecutor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClientChannelManager {

    private final ConcurrentMap<String, ClientChannelGroup> channelMap = new ConcurrentHashMap<>();

    public ClientChannelManager(Set<String> topics) {
        for (String topic : topics) {
            this.channelMap.put(topic, new ClientChannelGroup(topic, GlobalEventExecutor.INSTANCE));
        }
    }

    public boolean addChannel(String topic, Channel channel) {
        return channelMap.get(topic).add(channel);
    }

    public ChannelGroupFuture writeAndFlush(String topic, Message msg) {
        return channelMap.get(topic).writeAndFlush(msg);
    }

    public Set<String> getTopics() {
        return new HashSet<String>(channelMap.keySet());
    }

    public int getConnectionCount(String topic) {
        return channelMap.get(topic).size();
    }

    public int getAllConnectionCount() {
        int connectionCount = 0;
        for (ClientChannelGroup channelGroup : channelMap.values()) {
            connectionCount += channelGroup.size();
        }
        return connectionCount;
    }

}
