package com.github.shoreviewanalytics.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.github.shoreviewanalytics.kafka.producer.Video;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Vector;

public class CassandraVideoWriter {

    private static Logger logger = LoggerFactory.getLogger(CassandraVideoWriter.class);

    public void WriteToCassandra(Vector records) throws Exception {

        CassandraConnector connector = new CassandraConnector();
        connector.connect("cassandra-23daba12-shoreviewanalytics-d9c3.aivencloud.com", 12641, "aiven");
        CqlSession session = connector.getSession();


        try {
            CassandraDDL cassandraDDL = new CassandraDDL(session);
            cassandraDDL.createKeyspace("KAFKA_EXAMPLES", 1);
            //cassandraDDL.useKeyspace("KAFKA_EXAMPLES");
            cassandraDDL.createVideoTable("KAFKA_EXAMPLES", "VIDEOS_BY_TITLE_YEAR");
        } catch (com.datastax.oss.driver.api.core.servererrors.QueryExecutionException ex) {
            logger.info(ex.getMessage());
        }


        int counter = 0;
        for (Object record : records) {
            Video videos = (Video) record;
            counter = counter + 1;


            session.execute("INSERT INTO KAFKA_EXAMPLES.VIDEOS_BY_TITLE_YEAR(TITLE,ADDED_YEAR,ADDED_DATE,DESCRIPTION,USER_ID,VIDEO_ID) " + "" +
                    "VALUES('" + videos.getTitle() + "'," + videos.getAdded_year() + ",'" + videos.getAdded_date() + "','" + videos.getDescription() + "'," +
                    "" + videos.getUserid() + "," + videos.getVideoid() + ");");

        }
        logger.info("Total records inserted: " + counter);

        logger.info("Finished inserting records");
    }


}
