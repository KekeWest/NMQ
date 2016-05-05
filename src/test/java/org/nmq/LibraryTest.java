package org.nmq;

import static org.junit.Assert.*;

import org.junit.Test;
import org.nmq.enums.ChannelType;

public class LibraryTest {
  @Test
  public void test() throws InterruptedException {
    Channel channel = Channel.builder().channelType(ChannelType.PUB).port(10080).build();
    channel.start();
    channel.shutdown();
    assertTrue("someLibraryMethod should return 'true'", true);
  }
}
