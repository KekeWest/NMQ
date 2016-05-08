package org.nmq;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class MessageHandler extends SimpleChannelInboundHandler<Message> {

    protected final Channel channel;

    public MessageHandler(Channel channel) {
        super();
        this.channel = channel;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg) throws Exception {
        if (this.channel.isServer()) {
        } else {

        }
    }

}
