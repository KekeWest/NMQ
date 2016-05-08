package org.nmq;

import java.util.List;

import org.nmq.enums.ChannelType;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.Builder;
import lombok.Getter;

public class Channel {

    protected static final int DEFAULT_DATA_SIZE = 1048576;

    @Getter protected final ChannelType channelType;
    @Getter protected final QueueManager queueManager;
    @Getter protected final ChannelHandlerContextManager channelManager;
    protected final String address;
    protected final Integer port;

    @Getter protected final boolean server;
    protected io.netty.channel.Channel channel = null;
    protected EventLoopGroup acceptorGroup = null;
    protected EventLoopGroup workerGroup = null;

    /**
     * Create a new channel.
     *
     * @param channelType
     *        the {@link ChannelType} of the channel.
     * @param topics
     *        the topic for clients when the channel is a server.
     * @param capacity
     *        the capacity of the queue.
     * @param address
     *        the address to be connected to the server when the channel is a client.
     * @param port
     *        the port which the channel uses.
     */
    @Builder
    public Channel(ChannelType channelType, List<String> topics, Integer capacity,
            String address, Integer port) {
        this.channelType = channelType;
        this.queueManager = new QueueManager(topics, capacity);
        this.channelManager = new ChannelHandlerContextManager(topics);
        this.address = address;
        this.port = port;

        switch (this.channelType) {
        case PUB:
        case PUSH:
            this.server = true;
            break;
        case SUB:
        case PULL:
            this.server = false;
            break;
        default:
            throw new IllegalStateException("Unsupported channel type: " + this.channelType.name());
        }
    }

    /**
     * start the channel.
     *
     * @throws InterruptedException
     *         if the current thread was interrupted.
     */
    public void start() throws InterruptedException {
        if (this.isServer()) {
            bind();
        } else {
            connect();
        }
    }

    /**
     * shutdown the channel.
     */
    public void shutdown() {
        if (this.channel == null) {
            throw new IllegalStateException("Channel does not start");
        }

        this.channel.close();
        if (this.acceptorGroup != null) {
            this.acceptorGroup.shutdownGracefully();
        }
        this.workerGroup.shutdownGracefully();
    }

    protected void bind() throws InterruptedException {
        this.acceptorGroup = new NioEventLoopGroup(1);
        this.workerGroup = new NioEventLoopGroup();
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(this.acceptorGroup, this.workerGroup);
        serverBootstrap.localAddress(this.port);
        serverBootstrap.channel(getServerChannelClass());
        serverBootstrap.childHandler(getChannelInitializer());
        this.channel = serverBootstrap.bind().sync().channel();
    }

    protected void connect() throws InterruptedException {
        this.workerGroup = new NioEventLoopGroup();
        Bootstrap clientBootstrap = new Bootstrap();
        clientBootstrap.group(this.workerGroup);
        clientBootstrap.remoteAddress(this.address, this.port);
        clientBootstrap.channel(getClientChannelClass());
        clientBootstrap.handler(getChannelInitializer());
        this.channel = clientBootstrap.connect().sync().channel();
    }

    protected Class<? extends ServerChannel> getServerChannelClass() {
        return NioServerSocketChannel.class;
    }

    protected Class<? extends io.netty.channel.Channel> getClientChannelClass() {
        return NioSocketChannel.class;
    }

    protected ChannelInitializer<? extends io.netty.channel.Channel> getChannelInitializer() {
        int dataSize = DEFAULT_DATA_SIZE;
        return new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(
                    new MessageEncoder(),
                    new MessageDecoder(dataSize));
            }
        };
    }

}
