package org.nmq.channelhandler;

import org.nmq.Channel;
import org.nmq.ClientChannelManager;
import org.nmq.Message;
import org.nmq.request.RegistrationRequest;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ServerMessageHandler extends SimpleChannelInboundHandler<Message> {

    protected final Kryo kryo = new Kryo();

    protected final Channel channel;
    protected final ClientChannelManager channelManager;

    public ServerMessageHandler(Channel channel, ClientChannelManager channelManager) {
        super();
        this.channel = channel;
        this.channelManager = channelManager;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        Input input = new Input(msg.getBytes());
        Object request = kryo.readClassAndObject(input);
        input.close();

        if (request instanceof RegistrationRequest) {
            addChannel(ctx.channel(), (RegistrationRequest) request);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        ctx.close();
    }

    protected void addChannel(io.netty.channel.Channel channel, RegistrationRequest request) {
        for (String topic : request.getTopics()) {
            try {
                channelManager.addChannel(topic, channel);
            } catch (Exception e) {
            }
        }
    }

}
