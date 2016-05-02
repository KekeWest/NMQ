package org.nmq;

import org.nmq.enums.ChannelType;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;

public class NMQChannel {

    protected final ChannelConfig config;
    protected final ServerBootstrap serverBootstrap = null;
    protected final Bootstrap bootstrap = null;


    public NMQChannel(ChannelType channelType, String addr, int port, String... topics) {
        this.config = createConfig(channelType, addr, port, topics);
    }

    public NMQChannel(ChannelConfig config) {
        this.config = config;
    }

    public ChannelConfig createConfig(ChannelType channelType, String addr, int port, String[] topics) {
        return null;
    }

    public void start() {

    }

    public void close() {

    }
}
