package org.nmq.benchmarks;

import static java.lang.System.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.nmq.Channel;
import org.nmq.enums.ChannelType;

public class PubSubBenchmark {

    public static void main(String[] args) throws InterruptedException {
        Set<String> topics = new HashSet<>(Arrays.asList("test_topic"));
        int port = 10080;
        String address = "localhost";

        byte[] sendData = new byte[100];
        for (int i = 0; i < sendData.length; i++) {
            sendData[i] = 0;
        }

        Channel server = Channel.builder()
            .channelType(ChannelType.PUB)
            .topics(topics)
            .port(port)
            .build();
        server.start();

        Channel client = Channel.builder()
            .channelType(ChannelType.SUB)
            .topics(topics)
            .address(address)
            .port(port)
            .build();
        client.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    client.shutdown(true);
                    server.shutdown(true);
                    out.println("Channel has been shut down.");
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        });

        out.print("connecting... ");
        while (true) {
            if (server.getConnectionCount("test_topic") == 1)
                break;
        }
        out.println("ok");
        out.println("start Benchmark");

        PerformanceMeter meter = new PerformanceMeter("msg");
        meter.start();
        while (true) {
            server.send("test_topic", sendData);
            while (true) {
                byte[] recvBytes = client.receive("test_topic");
                if (recvBytes != null) {
                    meter.inc();
                    break;
                }
            }
        }
    }

}
