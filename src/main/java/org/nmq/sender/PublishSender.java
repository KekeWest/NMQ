package org.nmq.sender;

import org.nmq.ClientChannelManager;
import org.nmq.Message;
import org.nmq.QueueManager;

/**
 * TODO 速度向上のため、flushを非同期で行うようにするwriteAndAsyncFlush()メソッドをClientChannelGroupに実装する
 * TODO ChannelManagerから取得したClientChannelGroupを使ってwriteAndAsyncFlush()する方式に変える(そういう実装にしないと
 *      後でPushPullを実装するときに面倒なので)
 * @author keke
 *
 */
public class PublishSender extends MessageSender {

    public PublishSender(String topic, ClientChannelManager channelManager, QueueManager queueManager) {
        super(topic, channelManager, queueManager);
    }

    protected void send(Message msg) {
    }

}
