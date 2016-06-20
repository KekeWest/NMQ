package org.nmq.channelhandler;

import org.nmq.Message;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class MessageDecoder extends LengthFieldBasedFrameDecoder {

    public MessageDecoder(int maxDataSize) {
        super(maxDataSize, 0, 4, 0, 4);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = (ByteBuf) super.decode(ctx, in);
        if (frame == null) {
            return null;
        }
        Message msg = new Message();

        int topicStrLength = frame.readInt();
        byte[] byteTopicName = new byte[topicStrLength];
        frame.readBytes(byteTopicName);
        byte[] bytes = new byte[frame.readableBytes()];
        frame.readBytes(bytes);

        msg.setTopic(new String(byteTopicName, "UTF-8"));
        msg.setBytes(bytes);

        in.release();

        return msg;
    }

}
