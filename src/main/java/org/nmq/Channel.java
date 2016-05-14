package org.nmq;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.nmq.channelhandler.ClientMessageHandler;
import org.nmq.channelhandler.MessageDecoder;
import org.nmq.channelhandler.MessageEncoder;
import org.nmq.channelhandler.ServerMessageHandler;
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
import lombok.NonNull;

public class Channel {

    protected static final int DEFAULT_DATA_SIZE = 1048576;

    @Getter
    protected final ChannelType channelType;

    protected final Set<String> topics;
    protected final String address;
    protected final Integer port;
    protected final Integer capacity;

    @Getter
    protected final boolean server;
    protected final QueueManager queueManager;
    protected ClientChannelManager channelManager = null;
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
     * @param address
     *        the address to be connected to the server when the channel is a client.
     * @param port
     *        the port which the channel uses.
     * @param capacity
     *        the capacity of the queue.
     */
    @Builder
    public Channel(
        @NonNull ChannelType channelType,
        @NonNull List<String> topics,
        String address,
        Integer port,
        Integer capacity) {

        this.channelType = channelType;
        this.topics = new HashSet<>(topics);
        this.address = address;
        this.port = port;
        this.capacity = capacity;

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

        checkConfig();

        this.queueManager = new QueueManager(this.topics, this.capacity);

        if (this.isServer()) {
            this.channelManager = new ClientChannelManager(this.topics);
        }
    }

    protected void checkConfig() {
        if (topics.isEmpty()) {
            throw new IllegalArgumentException("Please set one or more topics");
        }
        if (topics.contains("")) {
            throw new IllegalArgumentException("Please do not set string of empty in a topic");
        }
        if (port < 0 || port > 0xFFFF) {
            throw new IllegalArgumentException("port out of range:" + port);
        }
        if (capacity != null && capacity <= 0) {
            throw new IllegalArgumentException("capacity is not greater than zero");
        }
        if (!this.isServer() && address == null) {
            throw new IllegalArgumentException("address can't be null");
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
        Channel thisChannel = this;
        return new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(
                    new MessageEncoder(),
                    new MessageDecoder(dataSize));

                if (thisChannel.isServer()) {
                    pipeline.addLast(new ServerMessageHandler(thisChannel, thisChannel.channelManager));
                } else {
                    pipeline.addLast(new ClientMessageHandler(thisChannel, thisChannel.topics));
                }
            }
        };
    }

}
