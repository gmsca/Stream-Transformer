package org.gms;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.json.JSONObject;

import java.util.*;

public class main {

    public static void main(String[] args) {
        SetupArguments(args);
        Topology topology = buildTopology();
        Properties props = buildProperties();

        final KafkaStreams streams = new KafkaStreams(topology, props);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static Properties buildProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, Arguments.ApplicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Arguments.Broker);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Arguments.AutoOffsetResetConfig);
        props.put(Arguments.SCHEMA_REGISTRY, Arguments.SchemaRegistryURL);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        return props;
    }

    private static Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        ArrayList<KTable<String, GenericRecord>> tables = KeySharedTableGenerator(builder);
        KTable<String, GenericRecord> joined = InnerJoinKTables(tables);
        joined.toStream().to(Arguments.OutputTopicName);

        return builder.build();
    }

    private static KTable<String, GenericRecord> InnerJoinKTables(ArrayList<KTable<String, GenericRecord>> tables) {
        KTable<String, GenericRecord> joined = tables.get(0);
        for (int i = 1; i < tables.size(); i++) {
            joined = joined.join(tables.get(i), (left,right) -> MergeMessages(left, right) );
        }
        return joined;
    }

    private static GenericRecord MergeMessages(GenericRecord left, GenericRecord right) {
        JSONObject mergedValues = MergeValues(left, right);
        ObjectMapper objectMapper = new ObjectMapper();
        return mergedGenericRecord;
    }

    private static JSONObject MergeValues(GenericRecord left, GenericRecord right) {
        JSONObject leftJSON = new JSONObject(left.toString());
        JSONObject rightJSON = new JSONObject(right.toString());
        leftJSON.keys().forEachRemaining(k -> {
            if (!rightJSON.has(k)) {
                rightJSON.put(k, leftJSON.get(k));
            }
        });
        return rightJSON;
    }

    private static String SetKey(GenericRecord value, String commonKey) {
        if (value==null) return null;
        else return value.get(commonKey).toString();
    }

    private static ArrayList<KTable<String, GenericRecord>> KeySharedTableGenerator(StreamsBuilder builder, String[] TopicNames, String commonKey) {
        ArrayList<KTable<String, GenericRecord>> tables = new ArrayList<>();
        for (String topicName : TopicNames) {
            KStream<String, GenericRecord> topic = builder.stream(topicName);
            KTable<String, GenericRecord> keySetTopic = topic.map((key, value) -> KeyValue.pair(SetKey(value, commonKey), value)).toTable();
            tables.add(keySetTopic);
        }
        return tables;
    }
    private static void SetupArguments(String[] args) {
        Arguments.Broker = args[2];
        Arguments.SchemaRegistryURL = args[3];
        Arguments.ApplicationID = args[4];
        Arguments.AutoOffsetResetConfig = args[5];
    }
}