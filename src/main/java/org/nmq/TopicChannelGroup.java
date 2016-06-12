package org.nmq;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ServerChannel;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.ChannelMatcher;
import io.netty.channel.group.ChannelMatchers;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.EventExecutor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TopicChannelGroup implements ChannelGroup {

    private final String topic;
    private final EventExecutor executor;
    private final CopyOnWriteArrayList<Channel> topicChannels = new CopyOnWriteArrayList<>();
    private final ChannelFutureListener remover = new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            remove(future.channel());
        }
    };
    private final boolean stayClosed;
    private volatile boolean closed;

    public TopicChannelGroup(String topic, EventExecutor executor) {
        this(topic, executor, false);
    }

    public TopicChannelGroup(String topic, EventExecutor executor, boolean stayClosed) {
        this.topic = topic;
        this.executor = executor;
        this.stayClosed = stayClosed;
    }

    @Override
    public int size() {
        return topicChannels.size();
    }

    @Override
    public boolean isEmpty() {
        return topicChannels.isEmpty();
    }

    @Override
    public boolean contains(Object channel) {
        return topicChannels.contains(channel);
    }

    @Override
    public Iterator<Channel> iterator() {
        return topicChannels.iterator();
    }

    @Override
    public Object[] toArray() {
        return topicChannels.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return topicChannels.toArray(a);
    }

    @Override
    public boolean add(Channel channel) {
        if (channel instanceof ServerChannel) {
            throw new IllegalArgumentException("channel cannot be ServerChannel.");
        }
        boolean added = topicChannels.add(channel);
        if (added) {
            channel.closeFuture().addListener(remover);
        }

        if (stayClosed && closed) {
            channel.close();
        }

        return added;
    }

    @Override
    public boolean remove(Object channel) {
        Channel c = (Channel) channel;
        if (c instanceof ServerChannel) {
            throw new IllegalArgumentException("channel cannot be ServerChannel.");
        }
        boolean removed = topicChannels.remove(c);
        if (!removed) {
            return false;
        }
        c.closeFuture().removeListener(remover);
        return true;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return topicChannels.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends Channel> c) {
        return topicChannels.addAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return topicChannels.retainAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return topicChannels.removeAll(c);
    }

    @Override
    public void clear() {
        topicChannels.clear();
    }

    @Override
    public int compareTo(ChannelGroup o) {
        int v = name().compareTo(o.name());
        if (v != 0) {
            return v;
        }

        return System.identityHashCode(this) - System.identityHashCode(o);
    }

    @Override
    public String name() {
        return topic;
    }

    @Override
    public ChannelGroupFuture write(Object message) {
        return write(message, ChannelMatchers.all());
    }

    private static Object safeDuplicate(Object message) {
        if (message instanceof ByteBuf) {
            return ((ByteBuf) message).duplicate().retain();
        } else if (message instanceof ByteBufHolder) {
            return ((ByteBufHolder) message).duplicate().retain();
        } else {
            return ReferenceCountUtil.retain(message);
        }
    }

    @Override
    public ChannelGroupFuture write(Object message, ChannelMatcher matcher) {
        if (message == null) {
            throw new NullPointerException("message");
        }
        if (matcher == null) {
            throw new NullPointerException("matcher");
        }

        Map<Channel, ChannelFuture> futures = new LinkedHashMap<Channel, ChannelFuture>(size());
        for (Channel c: topicChannels) {
            if (matcher.matches(c)) {
                futures.put(c, c.write(safeDuplicate(message)));
            }
        }

        ReferenceCountUtil.release(message);
        return new TopicChannelGroupFuture(this, futures, executor);
    }

    @Override
    public ChannelGroup flush() {
        return flush(ChannelMatchers.all());
    }

    @Override
    public ChannelGroup flush(ChannelMatcher matcher) {
        for (Channel c: topicChannels) {
            if (matcher.matches(c)) {
                c.flush();
            }
        }
        return this;
    }

    @Override
    public ChannelGroupFuture writeAndFlush(Object message) {
        return writeAndFlush(message, ChannelMatchers.all());
    }

    @Override
    public ChannelGroupFuture flushAndWrite(Object message) {
        return writeAndFlush(message, ChannelMatchers.all());
    }

    @Override
    public ChannelGroupFuture writeAndFlush(Object message, ChannelMatcher matcher) {
        if (message == null) {
            throw new NullPointerException("message");
        }

        Map<Channel, ChannelFuture> futures = new LinkedHashMap<Channel, ChannelFuture>(size());

        for (Channel c: topicChannels) {
            if (matcher.matches(c)) {
                futures.put(c, c.writeAndFlush(safeDuplicate(message)));
            }
        }

        ReferenceCountUtil.release(message);

        return new TopicChannelGroupFuture(this, futures, executor);
    }

    @Override
    public ChannelGroupFuture flushAndWrite(Object message, ChannelMatcher matcher) {
        return writeAndFlush(message, matcher);
    }

    @Override
    public ChannelGroupFuture disconnect() {
        return disconnect(ChannelMatchers.all());
    }

    @Override
    public ChannelGroupFuture disconnect(ChannelMatcher matcher) {
        if (matcher == null) {
            throw new NullPointerException("matcher");
        }

        Map<Channel, ChannelFuture> futures =
                new LinkedHashMap<Channel, ChannelFuture>(size());

        for (Channel c: topicChannels) {
            if (matcher.matches(c)) {
                futures.put(c, c.disconnect());
            }
        }

        return new TopicChannelGroupFuture(this, futures, executor);
    }

    @Override
    public ChannelGroupFuture close() {
        return close(ChannelMatchers.all());
    }

    @Override
    public ChannelGroupFuture close(ChannelMatcher matcher) {
        if (matcher == null) {
            throw new NullPointerException("matcher");
        }

        Map<Channel, ChannelFuture> futures =
                new LinkedHashMap<Channel, ChannelFuture>(size());

        if (stayClosed) {
            closed = true;
        }

        for (Channel c: topicChannels) {
            if (matcher.matches(c)) {
                futures.put(c, c.close());
            }
        }

        return new TopicChannelGroupFuture(this, futures, executor);
    }

    @Override
    public ChannelGroupFuture deregister() {
        return deregister(ChannelMatchers.all());
    }

    @Override
    public ChannelGroupFuture deregister(ChannelMatcher matcher) {
        if (matcher == null) {
            throw new NullPointerException("matcher");
        }

        Map<Channel, ChannelFuture> futures =
                new LinkedHashMap<Channel, ChannelFuture>(size());

        for (Channel c: topicChannels) {
            if (matcher.matches(c)) {
                futures.put(c, c.deregister());
            }
        }

        return new TopicChannelGroupFuture(this, futures, executor);
    }

}
