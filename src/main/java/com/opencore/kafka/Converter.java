package com.opencore.kafka;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class Converter {
  private static Logger logger = LoggerFactory.getLogger(SendFuture.class);
  private static Schema flugSchema;
  private static DatumWriter<GenericRecord> datumWriter = null;
  private static DocumentBuilder docBuilder;

  public static void main(String[] args) {
    String topicIn = args[0];
    String topicOut = args[1];

    String consumerGroup = "fraport9";

    // ===========
    // Generic Record Builder aus Schema initialisieren
    // ===========
    ClassLoader classLoader = SendFuture.class.getClassLoader();
    FileInputStream fstream = null;
    try {
      // Schema aus Datei laden - kann später dann Schema Registry
      fstream = new FileInputStream(classLoader.getResource("avro/Flug.avsc").getPath());
      flugSchema = Schema.parse(fstream); // Schema parsen (deprecated
      datumWriter = new GenericDatumWriter<GenericRecord>(flugSchema);
    } catch (NullPointerException | IOException e) {
      logger.error("Error reading schema from file avro/Flug.avsc: " + e.getMessage());
      System.exit(1);
    }

    // ===========
    // XML Parser initialisieren
    // ===========
    try {
      DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory
          .newInstance();
      docBuilder = docBuilderFactory.newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      logger.error("Error creating xml parser: " + e.getMessage());
      System.exit(1);
    }

    // ===========
    // Create producer object
    // ===========
    Properties producerProps = new Properties();
    producerProps.put("bootstrap.servers", "127.0.0.1:9092"); // Kafka Broker
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProps.put("acks", "all");
    producerProps.put("enable.idempotence", "true");
    producerProps.put("transactional.id", "kafkatest");
    KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps);

    // Initialisiert producer id und macht diese dem Cluster bekannt
    producer.initTransactions();

    // ===========
    // Consumer code
    // ===========

    // exemplarisch in einer Schleife, dies müßte für euren Code so umgebaut werden, dass
    // ihr regelmäßig nachfragt, ob es neue Nachrichten gibt
    Properties consumerProperties = new Properties();
    consumerProperties.put("bootstrap.servers", "127.0.0.1:9092"); // der Kafka Broker der angesprochen werden soll
    consumerProperties.put("auto.offset.reset", "earliest"); // gibt an, ob ihr initial Daten erhaltet oder am Ende des Topics zu lesen anfangt, wenn eine Consumer Group das allererste Mal verwendet wird
    consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); // wenn wir zu json serialisieren können wir diesen einfach als String übertragen
    consumerProperties.put("group.id",consumerGroup); // Consumer Group
    consumerProperties.put("enable.auto.commit","false"); // Automatischen Offset Commit abschalten, dies machen wir im Rahmen der Transaktion
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProperties);

    consumer.subscribe(Collections.singletonList(topicIn));

    ArrayList<Future> sends = new ArrayList<>();

    // Map um vorzuhalten, welche Offsets bereits verarbeitet wurden, damit diese als Teil der Transaktion
    // committed werden können
    Map<TopicPartition, OffsetAndMetadata> writtenOffsets = new HashMap<>();

    // 10 Mal nach neuen Nachrichten fragen (Batch kommt zurück, können also mehr Nachrichten sein)
    for (int i = 0; i < 10; i++) {
      writtenOffsets.clear();
      ConsumerRecords<String, String> records = consumer.poll(500); // nach neuen Records fragen
      if (!records.isEmpty()) {
        logger.info("Writing " + records.count() + " records to topic " + topicOut);

        // Transaktion starten
        producer.beginTransaction();
        for (ConsumerRecord<String, String> record : records) {
          // Konvertieren
          byte[] convertedRecord = convertRecord(record.value());
          // Record senden wenn != null (wenn == null dann trat bei der Konvertierung ein Fehler auf und wir ignorieren ihn)
          if (convertedRecord != null) {
            sends.add(producer.send(new ProducerRecord<String, byte[]>(topicOut, null, convertRecord(record.value()))));
          }
          // Größten Offset für die aktuelle Partition updaten (auch wenn wir nicht gesendet haben, denn gelesen haben wir ja
          // +1 um beim nächsten Lauf nicht den letzten Record erneut zu lesen sondern an der nächsten Position anzusetzen
          writtenOffsets.put(new TopicPartition(topicIn, record.partition()), new OffsetAndMetadata(record.offset() + 1));
        }
        // Offsets der Transaktion hinzufügen
        producer.sendOffsetsToTransaction(writtenOffsets,consumerGroup);

        // Transaktion abschließen - laut Doku erledigt dies auch den Flush und Check auf Exceptions, ich habe da aber
        // Zweifel ob hier nicht doch Fehler durchrutschen - müßte man nochmal genauer betrachten
        producer.commitTransaction();
      }
    }
    logger.info("Alle Records gesendet!");
  }

  private static byte[] convertRecord(String xmlRecord) {
    GenericRecordBuilder builder = new GenericRecordBuilder(flugSchema);

    Document document = null;
    try {
      // XML Record parsen
      document = docBuilder.parse(IOUtils.toInputStream(xmlRecord, "UTF-8"));
    } catch (SAXException | IOException e) {
      logger.warn("Error parsing record \"" + xmlRecord.substring(0,25) + "\" - record will be skipped:" + e.getMessage());
      return null;
    }

    NodeList nodeList = document.getElementsByTagName("*");
    // Über alle Felder des XML Records iterieren
    for (int i = 0; i < nodeList.getLength(); i++) {
      Node node = nodeList.item(i);
      if (node.getNodeType() == Node.ELEMENT_NODE) {
        // Prüfen ob das Avro Schema ein entsprechendes Feld enthält
        if (flugSchema.getField(node.getNodeName()) != null) {
          // Typ prüfen und Wert entsprechend konvertieren falls notwendig und das Avro Feld setzen
          // (bisher nur Int implementiert)
          if (flugSchema.getField(node.getNodeName()).schema().getType() == Schema.Type.INT) {
            try {
              builder.set(node.getNodeName(), Integer.parseInt(node.getFirstChild().getNodeValue()));
            } catch (NumberFormatException e) {
              logger.warn("Error parsing numeric field \"" + node.getNodeName() + "\" - record will be skipped:" + e.getMessage());
              return null;
            }
          } else {
            builder.set(node.getNodeName(), node.getFirstChild().getNodeValue());
          }
        } else {
          logger.warn("XML record contained field that is not known in the Avro schema: " + node.getNodeName());
        }
      }
    }
    GenericRecord flug = null;

    try {
      // Avro Record erzeugen
      flug = builder.build();
    } catch (Exception e) {
      logger.warn("Error creating Avro record - will be skipped: " + e.getMessage());
      return null;
    }

    // Record serialisieren und zurückgeben
    try {
      return serialize(flug);
    } catch (Exception e) {
      logger.warn("Error serializing record \"" + flug.toString() + "\" - record will be skipped:" + e.getMessage());
      return null;
    }
  }

  private static byte[] serialize(GenericRecord record) throws Exception {
    BinaryEncoder encoder = null;

    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    encoder = EncoderFactory.get().binaryEncoder(stream, encoder);

    datumWriter.write(record, encoder);
    encoder.flush();

    return stream.toByteArray();
  }

}




