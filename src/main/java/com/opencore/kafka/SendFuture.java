package com.opencore.kafka;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 */
public class SendFuture {
  private static Logger logger = LoggerFactory.getLogger(SendFuture.class);

  public static void main(String[] args) {
    String topic = args[0];
    String inputFile = args[1];

    // ===========
    // Create producer object
    // ===========
    Properties producerProps = new Properties();
    producerProps.put("bootstrap.servers", "127.0.0.1:9092"); // Kafka Broker
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put("acks", "all");
    KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

    // ===========
    // Synchronous sending of messages
    // ===========

    // For this we keep the Future object the producer returns and check these after
    // performing a flush on the produces (which blocks until they are all complete)
    ArrayList<Future> sends = new ArrayList<>();

    //Get file from resources folder
    ClassLoader classLoader = SendFuture.class.getClassLoader();
    FileInputStream fstream = null;
    try {
      fstream = new FileInputStream(classLoader.getResource(inputFile).getPath());
    } catch (FileNotFoundException | NullPointerException e) {
      logger.error("Error opening file xmlrecords.txt for reading: " + e.getMessage());
      System.exit(1);
    }
    BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

    //Read File Line By Line
    String strLine;
    try {
      while ((strLine = br.readLine()) != null) {
        sends.add(producer.send(new ProducerRecord<String, String>(topic, null, strLine)));
      }
      //Close the input stream
      br.close();
    } catch (IOException e) {
      logger.error("Error reading from file: " + e.getMessage());
    }


    // Send all queued records and block till every request either succeeded or failed
    producer.flush();

    // Check all futures, if an exception occured during sending the message it will be rethrown
    // when .get() is called
    for (Future result : sends) {
      try {
        result.get();
      } catch (InterruptedException | ExecutionException e) {
        logger.error("error during send: " + e.getMessage());
        System.exit(1);
      }
    }
    logger.info("All records sent synchroneously!");
  }
}
