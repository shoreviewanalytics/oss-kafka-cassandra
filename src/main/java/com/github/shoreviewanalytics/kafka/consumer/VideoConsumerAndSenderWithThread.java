package com.github.shoreviewanalytics.kafka.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.shoreviewanalytics.cassandra.CassandraVideoWriter;
import com.github.shoreviewanalytics.kafka.producer.Video;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import java.util.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class VideoConsumerAndSenderWithThread {

    private static final Logger logger = LoggerFactory.getLogger(VideoConsumerAndSenderWithThread.class);
    private static final Vector<Video> vectorOfVideos = new Vector<>();

    public static void consume(String brokers, String groupId, String topicName) {
        new VideoConsumerAndSenderWithThread().run(brokers,groupId, topicName);

    }

    private VideoConsumerAndSenderWithThread() {
    }

    private void run(String brokers, String groupId, String topicName) {

        // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);
        CassandraVideoWriter cassandraWriter = new CassandraVideoWriter();


        // create the consumer runnable
        logger.info("Creating the consumer thread");
        ConsumerThreadRunnable ConsumerRunnable = new ConsumerThreadRunnable(
                brokers,
                groupId,
                topicName,
                latch

        );

        // start the thread
        Thread sThread = new Thread(ConsumerRunnable);
        sThread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ConsumerRunnable.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }

        ));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }

    public static class ConsumerThreadRunnable implements Runnable {

        //private CountDownLatch latch;
        private KafkaConsumer<String, JsonNode> consumer;


        CountDownLatch latch = new CountDownLatch(1);

        ConsumerThreadRunnable(String brokers, String groupId, String topicName, CountDownLatch latch ) {
            this.latch = latch;
            Properties props = new Properties();

            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
            props.put("security.protocol", "SSL");
            props.put("ssl.endpoint.identification.algorithm", "");
            props.put("ssl.truststore.location", "/home/kafka/Downloads/kafka.service/client.truststore.jks");
            props.put("ssl.truststore.password", "audiovox1");
            props.put("ssl.keystore.type", "PKCS12");
            props.put("ssl.keystore.location", "/home/kafka/Downloads/kafka.service/client.keystore.p12");
            props.put("ssl.keystore.password", "audiovox1");
            props.put("ssl.key.password", "audiovox1");
            props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonDeserializer");
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


            consumer = new KafkaConsumer<>(props);

            //consumer.subscribe(Collections.singleton(topicName));
            //or multiple topics comma delimited
            consumer.subscribe(Arrays.asList(topicName));
            // get data


        }

        @Override
        public void run() {

            CassandraVideoWriter cassandraWriter = new CassandraVideoWriter();

             try {

                 int numberOfMessagesToRead = 429;  // kafka stores messages from 0 to ...n
                 boolean keepOnReading = true;
                 int numberOfMessagesReadSoFar = 0;

                 // poll for new data, get data
                 while (keepOnReading) {

                    ConsumerRecords<String, JsonNode> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, JsonNode> record : records) {
                        logger.info("Value: " + record.value());
                        //logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                        JsonNode jsonNode = record.value();

                        //logger.info("the jsonNode value for Title is" + jsonNode.get("title").asText());
                        Video video_record = new Video();

                        video_record.setTitle(jsonNode.get("title").asText());
                        video_record.setAdded_year(jsonNode.get("added_year").asText());
                        video_record.setAdded_date(jsonNode.get("added_date").asText());
                        video_record.setDescription(jsonNode.get("description").asText());
                        video_record.setUserid(jsonNode.get("userid").asText());
                        video_record.setVideoid(jsonNode.get("videoid").asText());

                        vectorOfVideos.add(video_record);

                        logger.info("The number of messages read so far are " + numberOfMessagesReadSoFar);

                        if (numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                            cassandraWriter.WriteToCassandra(vectorOfVideos);
                            keepOnReading = false;
                            break;
                        }
                        numberOfMessagesReadSoFar = numberOfMessagesReadSoFar + 1;

                    }

                }

            } catch (WakeupException e) {
                logger.info("Received shutdown signal!"); // received shutdown signal so out of loop
            } catch (Exception e) {
                 e.printStackTrace();
             } finally {
                consumer.close();
                latch.countDown(); // allows main code to understand able to exit
            }

        }

        void shutdown() {
            consumer.wakeup(); // method to interrupt consumer.poll() will throw exception wakeup exception

        }
    }


}
