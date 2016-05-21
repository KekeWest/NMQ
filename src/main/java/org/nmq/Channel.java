package org.nmq;

import java.util.HashSet;
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
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Channel {

    private static final int DEFAULT_DATA_SIZE = 1048576;

    @Getter
    private final ChannelType channelType;

    private final Set<String> topics;
    private final String address;
    private final Integer port;
    private final Integer capacity;

    @Getter
    private final boolean server;
    private final QueueManager queueManager;
    private final ClientChannelManager channelManager;
    private final MessageSendingManager messageSendingManager;
    @Getter
    private boolean started = false;
    private io.netty.channel.Channel channel = null;
    private EventLoopGroup acceptorGroup = null;
    private EventLoopGroup workerGroup = null;

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
        @NonNull Set<String> topics,
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
            this.messageSendingManager =
                new MessageSendingManager(channelType, channelManager, queueManager);
        } else {
            this.channelManager = null;
            this.messageSendingManager = null;
        }
    }

    private void checkConfig() {
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

    public Set<String> getTopics() {
        return new HashSet<>(topics);
    }

    public int getConnectionCount(String topic) {
        return channelManager.getConnectionCount(topic);
    }

    public int getAllConnectionCount() {
        return channelManager.getAllConnectionCount();
    }

    /**
     * start the channel.
     *
     * @throws InterruptedException
     *         if the current thread was interrupted.
     */
    public void start() throws InterruptedException {
        if (this.isServer()) {
            messageSendingManager.start();
            bind();
        } else {
            connect();
        }
        started = true;
    }

    /**
     * shutdown the channel.
     * @throws InterruptedException
     */
    public void shutdown(boolean now) throws InterruptedException {
        if (!isStarted()) {
            throw new IllegalStateException("Channel does not start");
        }

        if (isServer()) {
            messageSendingManager.shutdown(now);
        }

        channel.close();
        if (acceptorGroup != null) {
            acceptorGroup.shutdownGracefully();
        }
        workerGroup.shutdownGracefully();
        started = false;
    }

    private void bind() throws InterruptedException {
        acceptorGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.group(acceptorGroup, workerGroup);
        serverBootstrap.localAddress(port);
        serverBootstrap.channel(getServerChannelClass());
        serverBootstrap.childHandler(getChannelInitializer());
        channel = serverBootstrap.bind().sync().channel();
    }

    private void connect() throws InterruptedException {
        workerGroup = new NioEventLoopGroup();
        Bootstrap clientBootstrap = new Bootstrap();
        clientBootstrap.group(workerGroup);
        clientBootstrap.remoteAddress(address, port);
        clientBootstrap.channel(getClientChannelClass());
        clientBootstrap.handler(getChannelInitializer());
        channel = clientBootstrap.connect().sync().channel();
    }

    private Class<? extends ServerChannel> getServerChannelClass() {
        return NioServerSocketChannel.class;
    }

    private Class<? extends io.netty.channel.Channel> getClientChannelClass() {
        return NioSocketChannel.class;
    }

    private ChannelInitializer<? extends io.netty.channel.Channel> getChannelInitializer() {
        int dataSize = DEFAULT_DATA_SIZE;
        return new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(
                    new MessageEncoder(),
                    new MessageDecoder(dataSize));

                if (isServer()) {
                    pipeline.addLast(new ServerMessageHandler(channelType, channelManager));
                } else {
                    pipeline.addLast(new ClientMessageHandler(channelType, topics));
                }
            }
        };
    }

}
