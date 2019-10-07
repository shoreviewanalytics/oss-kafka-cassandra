package com.github.shoreviewanalytics;

import com.github.shoreviewanalytics.kafka.consumer.VideoConsumerAndSenderWithThread;
import com.github.shoreviewanalytics.kafka.producer.VideoProducer;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

// starting producers or consumers
public class Run {
    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {

        if (args.length < 3) {
            usage();
        }
        // Get the brokers
        String brokers = args[2];
        String topicName = args[1];


        switch (args[0].toLowerCase()) {
            case "producer":
                switch ((args[3]).toLowerCase()) {

                    case "videoproducer":
                        VideoProducer.produce(brokers, topicName);
                        break;
                }

            case "consumer":
                switch ((args[3]).toLowerCase()) {

                    case "videoconsumerandsenderwiththread":
                        // Either a groupId was passed in, or we need a random one
                        String groupId;
                        if (args.length == 5) {
                            groupId = args[4];
                        } else {
                            System.out.println(args.length);
                            groupId = UUID.randomUUID().toString();
                        }
                        VideoConsumerAndSenderWithThread.consume(brokers, groupId, topicName);
                        break;
                }

            default:
                usage();
        }
        System.exit(0);
    }

    // Display usage
    public static void usage() {
        System.out.println("Usage:");
        System.out.println("java -jar oss-kafka-cassandra-1.0.jar <producer|consumer> <topicName> brokerhosts Class [optional:groupid]");
        System.exit(1);

    }
}
