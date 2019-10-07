package com.github.shoreviewanalytics.kafka.producer;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class CSVTester {



    public static void main(String[] args)  {

        try (

                //FileReader fileReader = new FileReader("/videos_by_title_year.csv");
                //BufferedReader reader = new BufferedReader(fileReader);

                InputStream is = CSVTester.class.getResourceAsStream("/videos_by_title_year.csv");
                BufferedReader reader = new BufferedReader(new InputStreamReader(is));

                CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withDelimiter('$'));
        ) {
            for (CSVRecord csvRecord : csvParser) {
                // Accessing Values by Column Index

                String title = csvRecord.get(0);
                String added_year = csvRecord.get(1);
                String added_date = csvRecord.get(2);
                String description = csvRecord.get(3);
                String userid = csvRecord.get(4);
                String videoid = csvRecord.get(5);

                System.out.println("Record No - " + csvRecord.getRecordNumber());
                System.out.println("---------------");
                System.out.println("Title : " + title);
                System.out.println("Added Year : " + added_year);
                System.out.println("Added Date : " + added_date);
                System.out.println("Description : " + description);
                System.out.println("UserId : " + userid);
                System.out.println("VideoId : " + videoid);
                System.out.println("---------------\n\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

