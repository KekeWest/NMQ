package org.nmq;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.netty.channel.Channel;
import io.netty.util.concurrent.GlobalEventExecutor;

public class ClientChannelManager {

    protected final ConcurrentMap<String, ClientChannelGroup> channelsMap;

    public ClientChannelManager(Set<String> topics) {
        channelsMap = new ConcurrentHashMap<>();
        for (String topic : topics) {
            channelsMap.put(topic, new ClientChannelGroup(topic, GlobalEventExecutor.INSTANCE));
        }
    }

    public boolean addChannel(String topic, Channel channel) {
        return getChannelGroup(topic).add(channel);
    }

    public boolean containsTopic(String topic) {
        return channelsMap.containsKey(topic);
    }

    public Channel[] getChannels(String topic) {
        return (Channel[]) getChannelGroup(topic).toArray();
    }

    protected ClientChannelGroup getChannelGroup(String topic) {
        ClientChannelGroup channelGroup = channelsMap.get(topic);
        if (channelGroup == null) {
            throw new IllegalArgumentException(topic + " not found.");
        }
        return channelGroup;
    }

}
