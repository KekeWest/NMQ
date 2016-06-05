package org.nmq;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Test;
import org.nmq.enums.ChannelType;

public class MessageSendingTest {

    private static final Set<String> TEST_TOPICS = new HashSet<>(Arrays.asList("test_topic1", "test_topic2"));
    private static final String TEST_ADDRESS = "localhost";
    private static final int TEST_PORT = 10080;
    private static final byte[] TEST_DATA = new byte[] { 1, 2, 3, 4, 5 };

    private Channel server;
    private Channel client1;
    private Channel client2;

    @After
    public void after() throws InterruptedException {
        client1.shutdown(true);
        client2.shutdown(true);
        server.shutdown(true);
    }

    @Test(timeout = 1000)
    public void pubSubTest() throws InterruptedException {
        server = createServerChannel(ChannelType.PUB);
        server.start();

        client1 = createClientChannel(ChannelType.SUB);
        client1.start();
        client2 = createClientChannel(ChannelType.SUB);
        client2.start();

        while (true) {
            if (server.getAllConnectionCount() == 4) break;
        }

        server.send("test_topic1", TEST_DATA);
        server.send("test_topic2", TEST_DATA);

        byte[] client1Topic1Actual = null;
        byte[] client1Topic2Actual = null;
        byte[] client2Topic1Actual = null;
        byte[] client2Topic2Actual = null;
        while (true) {
            if (client1Topic1Actual == null)
                client1Topic1Actual = client1.receive("test_topic1");
            if (client1Topic2Actual == null)
                client1Topic2Actual = client1.receive("test_topic2");
            if (client2Topic1Actual == null)
                client2Topic1Actual = client2.receive("test_topic1");
            if (client2Topic2Actual == null)
                client2Topic2Actual = client2.receive("test_topic2");

            if (client1Topic1Actual != null && client1Topic2Actual != null
                && client2Topic1Actual != null && client2Topic2Actual != null) {
                break;
            }
        }

        assertArrayEquals(TEST_DATA, client1Topic1Actual);
        assertArrayEquals(TEST_DATA, client1Topic2Actual);
        assertArrayEquals(TEST_DATA, client2Topic1Actual);
        assertArrayEquals(TEST_DATA, client2Topic2Actual);
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
