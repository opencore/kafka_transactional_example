package com.opencore.kafka;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SendCallback {

  private static Logger logger = LoggerFactory.getLogger(SendFuture.class);
  public static List<Exception> exceptionList = new ArrayList<>();

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


    // ===========
    // Asynchroneous sending of records
    // ===========
    // In this approach the futures are discarded, instead a callback is passed
    // into the send which will raise an error only if an Exception is returned


    //Read File Line By Line
    String strLine;
    try {
      while ((strLine = br.readLine()) != null) {
        producer.send(new ProducerRecord<String, String>(topic, null, strLine), new DestinationCallback());
      }
      //Close the input stream
      br.close();
    } catch (IOException e) {
      logger.error("Error reading from file: " + e.getMessage());
    }

    logger.info("All records sent asynchroneously - not confirmed yet..");

    // Flush to enforce waiting for all requests to finish - not necessary if this is a
    // long running job that continuously checks for exceptions while sending
    producer.flush();
    checkIfException();
    logger.info("Send confirmed!");
  }

  private static void addException(Exception e) {
    exceptionList.add(e);
  }

  private static void checkIfException() {
    // Check if any exception was stored by a callback - if yes, abort program
    if (exceptionList.size() > 0) {
      logger.error("Error occurred - first one was: " + exceptionList.get(0).getMessage());
      System.exit(1);
    }
  }

  private static class DestinationCallback implements Callback {
    private String id;

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
      if (exception != null) {
        // Example exception handling, add your own here
        addException(exception); // Store exception for next call of checkIfException() to retrieve
      }
    }
  }
}
