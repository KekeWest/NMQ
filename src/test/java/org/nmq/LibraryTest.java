package org.nmq;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.nmq.enums.ChannelType;


public class LibraryTest {
    @Test public void test() {
    	NMQChannel.builder().channelType(ChannelType.PUB).build();
        assertTrue("someLibraryMethod should return 'true'", true);
    }
}
