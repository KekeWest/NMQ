package org.nmq.channelhandler;

import java.util.Set;

import org.nmq.ClientChannelManager;
import org.nmq.Message;
import org.nmq.enums.ChannelType;
import org.nmq.request.RegistrationRequest;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ServerMessageHandler extends SimpleChannelInboundHandler<Message> {

    private final Kryo kryo = new Kryo();
    private final ChannelType channelType;
    private final ClientChannelManager channelManager;

    public ServerMessageHandler(ChannelType channelType, ClientChannelManager channelManager) {
        super();
        this.channelType = channelType;
        this.channelManager = channelManager;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        Input input = new Input(msg.getBytes());
        Object request = kryo.readClassAndObject(input);
        input.close();

        if (request instanceof RegistrationRequest) {
            boolean registered = register(ctx.channel(), (RegistrationRequest) request);
            if (!registered) {
                ctx.close();
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        ctx.close();
    }

    private boolean register(io.netty.channel.Channel channel, RegistrationRequest request) {
        switch (channelType) {
        case PUB:
            if (request.getChannelType() != ChannelType.SUB) {
                return false;
            }
            break;
        case PUSH:
            if (request.getChannelType() != ChannelType.PULL) {
                return false;
            }
            break;
        default:
            throw new IllegalStateException("Unsupported channel type: " + this.channelType.name());
        }

        addChannel(channel, request.getTopics());

        return true;
    }

    private void addChannel(io.netty.channel.Channel channel, Set<String> topics) {
        for (String topic : topics) {
            channelManager.addChannel(topic, channel);
        }
    }

}
