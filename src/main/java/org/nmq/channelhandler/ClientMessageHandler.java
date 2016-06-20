package org.nmq.channelhandler;

import java.util.Set;

import org.nmq.AsyncFlusher;
import org.nmq.ClientHandlerManager;
import org.nmq.Message;
import org.nmq.enums.ChannelType;
import org.nmq.request.RegistrationRequest;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClientMessageHandler extends SimpleChannelInboundHandler<Message> {

    private final AsyncFlusher flusher;
    private final Kryo kryo = new Kryo();
    private final ChannelType serverChannelType;
    private final io.netty.channel.Channel channel;
    private final ClientHandlerManager handlerManager;

    private Set<String> requestTopics = null;

    public ClientMessageHandler(ChannelType channelType, io.netty.channel.Channel channel,
        ClientHandlerManager handlerManager) {
        super();
        this.flusher = new AsyncFlusher(channel);
        this.serverChannelType = channelType;
        this.channel = channel;
        this.handlerManager = handlerManager;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        Input input = new Input(msg.getBytes());
        Object request = kryo.readClassAndObject(input);
        input.close();

        if (request instanceof RegistrationRequest) {
            boolean registered = register((RegistrationRequest) request);
            if (!registered) {
                ctx.close();
            }
        }
    }

    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        deregister();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        super.exceptionCaught(ctx, cause);
        ctx.close();
    }

    private boolean register(RegistrationRequest request) {
        switch (serverChannelType) {
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
            throw new IllegalStateException("Unsupported channel type: " + this.serverChannelType.name());
        }

        requestTopics = request.getTopics();
        for (String topic : requestTopics) {
            handlerManager.addHandler(topic, this);
        }

        return true;
    }

    private void deregister() {
        if (requestTopics == null) {
            return;
        }

        for (String topic : requestTopics) {
            handlerManager.removeHandler(topic, this);
        }
    }

    public void send(Message msg) {
        channel.write(msg);
        flusher.flush();
    }

}
