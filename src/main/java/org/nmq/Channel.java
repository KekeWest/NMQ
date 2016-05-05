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

public class Channel {

    protected static final int DEFAULT_DATA_SIZE = 1048576;

    private ChannelType channelType = null;
    private String listenTopic = null;
    private List<String> topics = null;
    private String address = null;
    private Integer port = null;

    private io.netty.channel.Channel channel = null;
    private EventLoopGroup acceptorGroup = null;
    private EventLoopGroup workerGroup = null;

    /**
     * Create a new channel.
     *
     * @param channelType
     *        the {@link ChannelType} of the channel
     * @param listenTopic
     *        the topic that the channel listens
     * @param topics
     *        the topic for clients when the channel is a server.
     * @param address
     *        the address to be connected to the server when the channel is a client.
     * @param port
     *        the port which the channel uses.
     */
    @Builder
    public Channel(ChannelType channelType, String listenTopic, List<String> topics,
            String address, Integer port) {
        this.channelType = channelType;
        this.listenTopic = listenTopic;
        this.topics = topics;
        this.address = address;
        this.port = port;
    }

    /**
     * start the channel.
     *
     * @throws InterruptedException
     *         if the current thread was interrupted.
     */
    public void start() throws InterruptedException {
        if (this.channelType == null) {
            throw new IllegalStateException("channel type has not been specified.");
        }

        switch (this.channelType) {
        case PUB:
        case PUSH:
            bind();
            break;
        case SUB:
        case PULL:
            connect();
            break;
        default:
            throw new IllegalStateException("Unsupported channel type: " + this.channelType.name());
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
        switch (this.channelType) {
        case PUB:
        case PUSH:
            return NioServerSocketChannel.class;
        default:
            throw new IllegalStateException("Unsupported socket type: " + this.channelType.name());
        }
    }

    protected Class<? extends io.netty.channel.Channel> getClientChannelClass() {
        switch (this.channelType) {
        case SUB:
        case PULL:
            return NioSocketChannel.class;
        default:
            throw new IllegalStateException("Unsupported socket type: " + this.channelType.name());
        }
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
