package org.nmq;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import io.netty.channel.Channel;
import io.netty.channel.EventLoop;

public class AsyncFlusher {

    private static final int DEFAULT_MAX_PENDING = 64;

    private final Channel channel;
    private final EventLoop eventLoop;
    private final int maxPending;

    private final AtomicIntegerFieldUpdater<AsyncFlusher> WOKEN = AtomicIntegerFieldUpdater
        .newUpdater(AsyncFlusher.class, "woken");
    @SuppressWarnings("UnusedDeclaration")
    private volatile int woken;

    private int pending;

    /**
     * Used to flush all outstanding writes in the outbound channel buffer.
     */
    private final Runnable flush = new Runnable() {
        @Override
        public void run() {
            pending = 0;
            channel.flush();
        }
    };

    /**
     * Used to wake up the event loop and schedule a flush to be performed after all outstanding write
     * tasks are run. The outstanding write tasks must be allowed to run before performing the actual
     * flush in order to ensure that their payloads have been written to the outbound buffer.
     */
    private final Runnable wakeup = new Runnable() {
        @Override
        public void run() {
            woken = 0;
            eventLoop.execute(flush);
        }
    };

    public AsyncFlusher(final Channel channel) {
        this(channel, DEFAULT_MAX_PENDING);
    }

    public AsyncFlusher(final Channel channel, final int maxPending) {
        this.channel = channel;
        this.maxPending = maxPending;
        this.eventLoop = channel.eventLoop();
    }

    /**
     * Schedule an asynchronous opportunistically batching flush.
     */
    public void flush() {
        pending++;
        if (pending >= maxPending) {
            pending = 0;
            channel.flush();
        }

        if (woken == 0 && WOKEN.compareAndSet(this, 0, 1)) {
            woken = 1;
            eventLoop.execute(wakeup);
        }
    }

}
