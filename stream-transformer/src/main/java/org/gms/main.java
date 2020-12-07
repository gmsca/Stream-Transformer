package org.gms;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.gms.claimclasses.*;
import org.json.JSONObject;

import java.util.*;

@JsonIgnoreProperties(ignoreUnknown = true)

public class main {

    // main is entrypoint
    public static void main(String[] args) {
        SetupArguments(args);
        Topology topology = buildTopology();
        Properties props = buildProperties();

        final KafkaStreams streams = new KafkaStreams(topology, props);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    // configure application properties
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

    // define how to transform input streams in build topology
    private static Topology buildTopology() {

        KStream<String, GenericRecord> ClaimStatus = GetKStream("CIMS.Financial.ClaimStatus");
        KTable<String, GenericRecord> ClaimStatus_set = ClaimStatus.map((key, value) -> KeyValue.pair(GetKey(value, "CS_ClaimStatusID"), value)).toTable();
        KStream<String, GenericRecord> ClaimStatusClaimLink = GetKStream("CIMS.Financial.ClaimStatusClaimLink");
        KTable<String, GenericRecord> ClaimStatusClaimLink_set = ClaimStatusClaimLink.map((key, value) -> KeyValue.pair(GetKey(value, "CS_ClaimStatusID"), value)).toTable();
        KTable<String, GenericRecord> ClaimStatus_ClaimStatusClaimLink =  ClaimStatus_set.leftJoin(ClaimStatusClaimLink_set, (left, right) -> MergeMessages(left, right, ClaimStatus_ClaimStatusClaimLink.class));

        KStream<String, GenericRecord> Claim = GetKStream("CIMS.Financial.Claim");
        KTable<String, GenericRecord> Claim_set = Claim.map((key, value) -> KeyValue.pair(GetKey(value, "CL_ClaimID"), value)).toTable();
        KTable<String, GenericRecord> ClaimStatus_ClaimStatusClaimLink_set = ClaimStatus_ClaimStatusClaimLink.toStream().map((key, value) -> KeyValue.pair(GetKey(value, "CL_ClaimID"), value)).toTable();
        KTable<String, GenericRecord> Claim_ClaimStatus_ClaimStatusClaimLink =  Claim_set.leftJoin(ClaimStatus_ClaimStatusClaimLink_set, (left, right) -> MergeMessages(left, right, Claim_ClaimStatus_ClaimStatusClaimLink.class));

        KStream<String, GenericRecord> ClaimState = GetKStream("CIMS.Reference.ClaimState");
        KStream<String, GenericRecord> Claim_ClaimStatus_ClaimStatusClaimLink_set = Claim_ClaimStatus_ClaimStatusClaimLink.toStream().map((key, value) -> KeyValue.pair(GetKey(value, "CB_ClaimStateID"), value));
        KTable<String, GenericRecord> ClaimState_set = ClaimState.map((key, value) -> KeyValue.pair(GetKey(value, "CB_ClaimStateID"), value)).toTable();
        KStream<String, GenericRecord> Claim_ClaimStatus_ClaimStatusClaimLink_ClaimState = Claim_ClaimStatus_ClaimStatusClaimLink_set.leftJoin(ClaimState_set, (left, right) -> MergeMessages(left, right, Claim_ClaimStatus_ClaimStatusClaimLink_ClaimState.class));

        Claim_ClaimStatus_ClaimStatusClaimLink_ClaimState.to(Arguments.OutputTopic);

        return Arguments.builder.build();
    }

    // get the key of a genericRecord
    private static String GetKey(GenericRecord value, String regex) {
        if (value==null || value.get(regex)==null) return null;
        else return value.get(regex).toString();
    }

    // return KStream from topic name
    private static KStream<String, GenericRecord> GetKStream(String topic) {
        return Arguments.builder.stream(topic);
    }

    // merge 2 genericRecords and return a new genericRecord using the defined output class
    private static GenericRecord MergeMessages(GenericRecord left, GenericRecord right, Class<?> Class) {
        String mergedValues = MergeValues(left, right);
        ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            return (GenericRecord) objectMapper.readValue(mergedValues, Class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

    // merge the contents of 2 genericRecords
    private static String MergeValues(GenericRecord left, GenericRecord right) {
        if (right==null) return left.toString();
        JSONObject leftJSON = new JSONObject(left.toString());
        JSONObject rightJSON = new JSONObject(right.toString());
        rightJSON.keys().forEachRemaining(k -> {
            if (!leftJSON.has(k)) {
                leftJSON.put(k, rightJSON.get(k));
            }
        });
        return leftJSON.toString();
    }

    // setup arguments for application
    private static void SetupArguments(String[] args) {
        Arguments.Broker = args[0];
        Arguments.SchemaRegistryURL = args[1];
        Arguments.ApplicationID = args[2];
        Arguments.OutputTopic = args[2];
        Arguments.AutoOffsetResetConfig = args[3];
    }
}