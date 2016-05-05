package org.nmq;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class MessageEncoder extends MessageToByteEncoder<Message> {

    private static final byte[] LENGTH_PLACEHOLDER = new byte[4];

    @Override
    protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out) throws Exception {
        int startIdx = out.writerIndex();

        out.writeBytes(LENGTH_PLACEHOLDER);
        byte[] byteTopicName = msg.getTopic().getBytes("UTF-8");
        out.writeInt(byteTopicName.length);
        out.writeBytes(byteTopicName);
        out.writeBytes(msg.getBytes());

        int endIdx = out.writerIndex();

        out.setInt(startIdx, endIdx - startIdx - 4);
    }

}
