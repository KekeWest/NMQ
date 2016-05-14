package org.nmq;

import io.netty.channel.Channel;
import io.netty.channel.ServerChannel;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.EventExecutor;

public class ClientChannelGroup extends DefaultChannelGroup {

    public ClientChannelGroup(String name, EventExecutor executor) {
        super(name, executor);
    }

    @Override
    public boolean add(Channel channel) {
        if (channel instanceof ServerChannel) {
            throw new IllegalArgumentException("channel cannot be ServerChannel.");
        }
        return super.add(channel);
    }

}
