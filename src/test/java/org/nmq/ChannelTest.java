package org.nmq;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.nmq.enums.ChannelType;

public class ChannelTest {

    public static final Set<String> TEST_TOPICS = new HashSet<>(Arrays.asList("test_topic1", "test_topic2"));
    public static final String TEST_ADDRESS = "localhost";
    public static final int TEST_PORT = 10080;

    @Test(timeout = 1000)
    public void connectionTest() throws InterruptedException, NoSuchFieldException, SecurityException {

        Channel server = Channel.builder()
            .channelType(ChannelType.PUB)
            .topics(TEST_TOPICS)
            .port(TEST_PORT)
            .build();
        server.start();

        Channel client = Channel.builder()
            .channelType(ChannelType.SUB)
            .topics(TEST_TOPICS)
            .address(TEST_ADDRESS)
            .port(TEST_PORT)
            .build();
        client.start();

        while (true) {
            if (server.getAllConnectionCount() == 2) {
                break;
            }
        }

        assertTrue("connectionTest failure.", server.getConnectionCount("test_topic1") == 1);
        assertTrue("connectionTest failure.", server.getConnectionCount("test_topic2") == 1);

        server.shutdown(true);
        client.shutdown(true);
    }
}
