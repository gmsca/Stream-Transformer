package io.confluent.developer.spring.avro;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import lombok.extern.apachecommons.CommonsLog;

@Service
@CommonsLog(topic = "Consumer Logger")
public class Consumer {

  @Autowired
  private SimpMessagingTemplate webSocket;

  @KafkaListener(topics = "test-ux", groupId = "ux-consumer")
  public void consume(ConsumerRecord<String, GenericRecord> record) {
    System.out.printf("Consumed message -> %s%n", record.value());
    webSocket.convertAndSend(Constants.WEBSOCKET_DESTINATION, record.value().toString());
  }
}