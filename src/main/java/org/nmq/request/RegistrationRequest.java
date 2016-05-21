package org.nmq.request;

import java.io.Serializable;
import java.util.Set;

import org.nmq.enums.ChannelType;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RegistrationRequest implements Serializable {

    private ChannelType channelType;

    private Set<String> topics;

}
