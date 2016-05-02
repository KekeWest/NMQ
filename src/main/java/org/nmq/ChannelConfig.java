package org.nmq;

import org.nmq.enums.ChannelType;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ChannelConfig {

    private final ChannelType channelType;

    private final String topic;

    private final int port;

    private final String address;

    private final String[] topics;

}
