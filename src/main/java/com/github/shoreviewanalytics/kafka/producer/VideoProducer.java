package com.github.shoreviewanalytics.kafka.producer;


import org.apache.commons.lang.StringEscapeUtils;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class VideoProducer {

    private static Logger logger = LoggerFactory.getLogger(VideoProducer.class);

    public static void produce(String brokers, String topicName)  {

        // Set properties used to configure the producer
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokers);
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        KafkaProducer<String, JsonNode> producer = new KafkaProducer<>(props);

        ObjectMapper objectMapper = new ObjectMapper();

        try (
                // FileReader fileReader = new FileReader("src/main/resources/videos_by_title_year.csv");
                // BufferedReader reader = new BufferedReader(fileReader);

                InputStream is = VideoProducer.class.getResourceAsStream("/videos_by_title_year.csv");
                BufferedReader reader = new BufferedReader(new InputStreamReader(is));

                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withDelimiter('$'))
        ) {
            for (CSVRecord csvRecord : csvParser) {
                // Accessing Values by Column Index

                String line = csvRecord.get(0) +","+csvRecord.get(1)+","+csvRecord.get(2)+","+csvRecord.get(3)+","+csvRecord.get(4)+","+csvRecord.get(5);
                Video video = new Video();
                video.setTitle(csvRecord.get(0));
                video.setAdded_year(csvRecord.get(1));
                video.setAdded_date(csvRecord.get(2));
                video.setDescription(csvRecord.get(3));
                video.setUserid(csvRecord.get(4));
                video.setVideoid(csvRecord.get(5));

                producer.send(new ProducerRecord<>(topicName, objectMapper.valueToTree(video)));

                logger.info(line);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("done producing video messages");
        producer.flush();
        producer.close();


    }







}
