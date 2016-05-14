package org.nmq;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.nmq.enums.ChannelType;

public class ChannelTest {

    public static final List<String> TEST_TOPICS = Arrays.asList("test_topic1", "test_topic2");
    public static final String TEST_ADDRESS = "localhost";
    public static final int TEST_PORT = 10080;

    @Test
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

        Thread.sleep(100);

        ClientChannelManager channelManager = server.channelManager;
        assertTrue("connectionTest failure.", channelManager.getChannelGroup("test_topic1").size() == 1);
    }
}
