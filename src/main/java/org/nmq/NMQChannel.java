package org.nmq;

import java.util.List;

import org.nmq.enums.ChannelType;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import lombok.Builder;

public class NMQChannel {

	private ChannelType channelType;
	private String listenTopic;
	private List<String> topics;
	private String address;
	private Integer port;

    @Builder
    public NMQChannel(ChannelType channelType, String listenTopic, List<String> topics,
    		String address, Integer port) {
    	this.channelType = channelType;
    	this.listenTopic = listenTopic;
    	this.topics = topics;
    	this.address = address;
    	this.port = port;
    }

    public void start() {
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
			throw new IllegalArgumentException("Unsupported socket type: " + this.channelType.name());
		}
    }

    protected void bind() {
    	EventLoopGroup bossGroup = new NioEventLoopGroup(1);
    	EventLoopGroup workerGroup = new NioEventLoopGroup();
    	 ServerBootstrap serverBootstrap = new ServerBootstrap();
    	 serverBootstrap.group(bossGroup, workerGroup)
    	 .channel(NioServerSocketChannel.class)
    	 .handler(new ChannelInitializer<SocketChannel>() {
    		 @Override
    		 public void initChannel(SocketChannel ch) throws Exception {
    			 ChannelPipeline p = ch.pipeline();
    			 p.addLast(
    					 new ObjectEncoder(),
    					 new ObjectDecoder(ClassResolvers.cacheDisabled(null))
    					 );
    		 }
    	 });

    }

    protected void connect() {

    }
}
