package org.nmq.request;

import java.io.Serializable;

import lombok.Data;

@Data
public class RegistrationRequest implements Serializable {

    private String topic;

}
