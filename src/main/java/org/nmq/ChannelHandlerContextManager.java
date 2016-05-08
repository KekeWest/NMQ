package org.nmq;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import io.netty.channel.ChannelHandlerContext;

public class ChannelHandlerContextManager {

    protected final ConcurrentHashMap<String, ArrayList<ChannelHandlerContext>> channelsMap;

    public ChannelHandlerContextManager(List<String> topics) {
        this.channelsMap = new ConcurrentHashMap<>();
        for (String topic : topics) {
            this.channelsMap.put(topic, new ArrayList<ChannelHandlerContext>());
        }
    }

    public boolean addChannel(String topic, ChannelHandlerContext channel) {
        return getChannels(topic).add(channel);
    }

    public boolean removeChannel(String topic, ChannelHandlerContext channel) {
        return getChannels(topic).remove(channel);
    }

    public ArrayList<ChannelHandlerContext> getChannels(String topic) {
        return this.channelsMap.get(topic);
    }

}
