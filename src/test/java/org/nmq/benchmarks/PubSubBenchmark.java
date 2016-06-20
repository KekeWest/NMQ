package org.nmq.benchmarks;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.nmq.Channel;
import org.nmq.enums.ChannelType;
import org.nmq.receiver.MessageReceiver;

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

        BenchmarkReceiver benchmarkReceiver = new BenchmarkReceiver(server);

        Channel client = Channel.builder()
            .channelType(ChannelType.SUB)
            .topics(topics)
            .address(address)
            .port(port)
            .build();
        client.setReceiver("test_topic", benchmarkReceiver);
        client.start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    client.shutdown(true);
                    server.shutdown(true);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        });

        while (true) {
            if (server.getConnectionCount("test_topic") == 1)
                break;
        }

        benchmarkReceiver.startMeter();
        for (int i = 0; i < 128; i++) {
            server.send("test_topic", sendData);
        }

        Thread.sleep(Long.MAX_VALUE);
    }

    private static class BenchmarkReceiver extends MessageReceiver {

        private final Channel server;
        private final PerformanceMeter meter;

        public BenchmarkReceiver(Channel server) {
            this.server = server;
            meter = new PerformanceMeter("msg");
        }

        public void startMeter() {
            meter.start();
        }

        @Override
        public void receivedMessage(byte[] bytes) {
            meter.inc();
            server.send("test_topic", bytes);
        }

    }

}
