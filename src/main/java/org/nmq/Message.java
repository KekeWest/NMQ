package org.nmq;

import java.io.Serializable;

import io.netty.buffer.ByteBuf;
import lombok.Data;

@Data
public class Message implements Serializable {

    private String topic;

    private ByteBuf bytes;

}
