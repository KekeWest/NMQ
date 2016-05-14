package org.nmq.channelhandler;

import java.io.ByteArrayOutputStream;
import java.util.Set;

import org.nmq.Channel;
import org.nmq.Message;
import org.nmq.request.RegistrationRequest;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClientMessageHandler extends SimpleChannelInboundHandler<Message> {

    protected final Kryo kryo = new Kryo();

    protected final Channel channel;
    protected final Set<String> topics;

    public ClientMessageHandler(Channel channel, Set<String> topics) {
        super();
        this.channel = channel;
        this.topics = topics;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        Output output = new Output(new ByteArrayOutputStream());
        RegistrationRequest request = new RegistrationRequest(topics);
        kryo.writeClassAndObject(output, request);
        Message msg = new Message("", output.toBytes());
        output.close();
        ctx.writeAndFlush(msg);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {

    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        ctx.close();
    }

}
