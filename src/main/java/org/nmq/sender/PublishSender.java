package org.nmq.sender;

import java.util.concurrent.BlockingQueue;

import org.nmq.Message;
import org.nmq.TopicHandlerGroup;
import org.nmq.channelhandler.ClientMessageHandler;

public class PublishSender extends MessageSender {

    public PublishSender(TopicHandlerGroup handlerGroup, BlockingQueue<Message> queue) {
        super(handlerGroup, queue);
    }

    protected void send(Message msg) {
        for (ClientMessageHandler handler : handlerGroup) {
            handler.send(msg);
        }
    }

}
