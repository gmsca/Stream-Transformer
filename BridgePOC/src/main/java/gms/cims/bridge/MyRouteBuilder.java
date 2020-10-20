package gms.cims.bridge;

import org.apache.camel.builder.RouteBuilder;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;

/**
 * A Camel Java DSL Router
 */
public class MyRouteBuilder extends RouteBuilder {

    /**
     * Let's configure the Camel routing rules using Java code...
     */
    public void configure() {
        TopicSetup().forEach(topic -> {
            from(topic).process(new MessageProcessor())
                    .log("Message received : ${body}")
                    .to("kafka:" + "CIMSTEST.Test" + "?brokers=" + Arguments.Broker
                        + "&serializerClass=" + KafkaAvroSerializer.class.getName());
        });
    }

    private ArrayList<String> TopicSetup(){
        ArrayList<String> configuredTopics = new ArrayList<>();
        Arguments.Topics.forEach(topic -> {
            configuredTopics.add("kafka:" + topic + "?brokers=" + Arguments.Broker
                    + "&maxPollRecords=5000"
                    + "&consumersCount=1"
                    + "&groupId=" + Arguments.GroupId
                    + "&keyDeserializer=" + StringDeserializer.class.getName()
                    + "&valueDeserializer=" + KafkaAvroDeserializer.class.getName());
        });
        return configuredTopics;
    }

}
