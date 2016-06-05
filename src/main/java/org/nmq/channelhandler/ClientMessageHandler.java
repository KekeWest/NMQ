package org.nmq.channelhandler;

import java.io.ByteArrayOutputStream;
import java.util.Set;

import org.nmq.Message;
import org.nmq.QueueManager;
import org.nmq.enums.ChannelType;
import org.nmq.request.RegistrationRequest;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClientMessageHandler extends SimpleChannelInboundHandler<Message> {

    private final Kryo kryo = new Kryo();
    private final ChannelType channelType;
    private final Set<String> topics;
    private final QueueManager queueManager;

    public ClientMessageHandler(ChannelType channelType, Set<String> topics, QueueManager queueManager) {
        super();
        this.channelType = channelType;
        this.topics = topics;
        this.queueManager = queueManager;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Output output = new Output(new ByteArrayOutputStream());
        RegistrationRequest request = new RegistrationRequest(channelType, topics);
        kryo.writeClassAndObject(output, request);
        Message msg = new Message("", output.toBytes());
        output.close();
        ctx.writeAndFlush(msg);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        queueManager.offer(msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        ctx.close();
    }

}
