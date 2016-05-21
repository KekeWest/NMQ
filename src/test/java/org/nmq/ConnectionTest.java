package org.nmq;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.nmq.enums.ChannelType;

public class ConnectionTest {

    protected static final Set<String> TEST_TOPICS = new HashSet<>(Arrays.asList("test_topic1", "test_topic2"));
    protected static final String TEST_ADDRESS = "localhost";
    protected static final int TEST_PORT = 10080;

    @Test(timeout = 1000)
    public void pubSubTest() throws InterruptedException {
        Channel server = createServerChannel(ChannelType.PUB);
        server.start();

        Channel client = createClientChannel(ChannelType.SUB);
        client.start();

        while (true) {
            if (server.getAllConnectionCount() == 2) break;
        }

        assertEquals(1, server.getConnectionCount("test_topic1"));
        assertEquals(1, server.getConnectionCount("test_topic2"));

        server.shutdown(true);
        client.shutdown(true);
    }

    @Test(timeout = 1000)
    public void pushPullTest() throws InterruptedException {
        Channel server = createServerChannel(ChannelType.PUSH);
        server.start();

        Channel client = createClientChannel(ChannelType.PULL);
        client.start();

        while (true) {
            if (server.getAllConnectionCount() == 2) break;
        }

        assertEquals(1, server.getConnectionCount("test_topic1"));
        assertEquals(1, server.getConnectionCount("test_topic2"));

        server.shutdown(true);
        client.shutdown(true);
    }

    private Channel createServerChannel(ChannelType channelType) {
        return Channel.builder()
            .channelType(channelType)
            .topics(TEST_TOPICS)
            .port(TEST_PORT)
            .build();
    }

    private Channel createClientChannel(ChannelType channelType) {
        return Channel.builder()
            .channelType(channelType)
            .topics(TEST_TOPICS)
            .address(TEST_ADDRESS)
            .port(TEST_PORT)
            .build();
    }

}
