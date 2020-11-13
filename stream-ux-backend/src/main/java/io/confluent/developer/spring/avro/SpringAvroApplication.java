package io.confluent.developer.spring.avro;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class SpringAvroApplication {
  public static void main(String[] args) {
    SpringApplication.run(SpringAvroApplication.class, args);
  }
}


