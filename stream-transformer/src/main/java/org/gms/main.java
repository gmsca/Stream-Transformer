package org.gms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KTable;
import org.gms.claimclasses.*;
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

        KTable<String, GenericRecord> ClaimCase_CaseNoteLink = CreateJoinedKTable(
                GetKTables(builder, "CIMSTEST.Financial.ClaimCase CIMSTEST.Financial.CaseNoteLink"),
                "CA_CaseID",
                ClaimCase_ClaimNoteLink.class
                );

        KTable<String, GenericRecord> ClaimStatus_ClaimStatusClaimLink = CreateJoinedKTable(
                GetKTables(builder, "CIMSTEST.Financial.ClaimStatusClaimLink CIMSTEST.Financial.ClaimStatus"),
                "CS_ClaimStatusID",
                ClaimStatus_ClaimStatusClaimLink.class
        );

        ArrayList<KTable<String, GenericRecord>> Claim_ClaimStatus_ClaimStatusClaimLink = GetKTables(builder, "CIMSTEST.Financial.Claim");
        Claim_ClaimStatus_ClaimStatusClaimLink.add(ClaimStatus_ClaimStatusClaimLink);
        KTable<String, GenericRecord> Claim_ClaimStatus_ClaimStatusClaimLink_joined = CreateJoinedKTable(Claim_ClaimStatus_ClaimStatusClaimLink, "CL_ClaimID", Claim_ClaimStatus_ClaimStatusClaimLink.class);

        ArrayList<KTable<String, GenericRecord>> Claim_ClaimStatus_ClaimStatusClaimLink_ClaimCase_CaseNoteLink_joined = new ArrayList<>();
        Claim_ClaimStatus_ClaimStatusClaimLink_ClaimCase_CaseNoteLink_joined.add(Claim_ClaimStatus_ClaimStatusClaimLink_joined);
        Claim_ClaimStatus_ClaimStatusClaimLink_ClaimCase_CaseNoteLink_joined.add(ClaimCase_CaseNoteLink);
        KTable<String, GenericRecord> final_join = CreateJoinedKTable(Claim_ClaimStatus_ClaimStatusClaimLink_ClaimCase_CaseNoteLink_joined, "CA_CaseID", Claim_ClaimStatus_ClaimStatusClaimLink_ClaimCase_ClaimNoteLink.class);

        final_join.toStream().to(Arguments.OutputTopic);

        return builder.build();
    }

    // return array of KTables from string of stream names
    private static ArrayList<KTable<String, GenericRecord>> GetKTables(StreamsBuilder builder, String topics) {
        ArrayList<KTable<String, GenericRecord>> tables = new ArrayList<>();
        for (String topic : topics.split(" ")) {
            tables.add(builder.table(topic));
        }
        return tables;
    }

    private static KTable<String, GenericRecord> CreateJoinedKTable(ArrayList<KTable<String, GenericRecord>> tables, String commonKey, Class<?> Class) {
        ArrayList<KTable<String, GenericRecord>> keySharedTables = KeySharedKTableGenerator(tables, commonKey);
        return LeftJoinKTables(keySharedTables, Class);

    }

    private static KTable<String, GenericRecord> LeftJoinKTables(ArrayList<KTable<String, GenericRecord>> kTables, Class<?> Class) {
        KTable<String, GenericRecord> joined = kTables.get(0);
        for (int i = 1; i < kTables.size(); i++) {
            joined = joined.leftJoin(kTables.get(i), (left, right) -> {
                try {
                    return MergeMessages(left, right, Class);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                    return null;
                }
            });
        }
        return joined;
    }

    private static GenericRecord MergeMessages(GenericRecord left, GenericRecord right, Class<?> Class) throws JsonProcessingException {
        String mergedValues = MergeValues(left, right);
        ObjectMapper objectMapper = new ObjectMapper();
        return (GenericRecord) objectMapper.readValue(mergedValues, Class);
    }

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

    private static String SetKey(GenericRecord value, String commonKey) {
        if (value==null) return null;
        return value.get(commonKey).toString();
    }

    private static ArrayList<KTable<String, GenericRecord>> KeySharedKTableGenerator(ArrayList<KTable<String, GenericRecord>> kTables, String commonKey) {
        ArrayList<KTable<String, GenericRecord>> keySharedKTables = new ArrayList<>();
        for (KTable<String, GenericRecord> kTable : kTables) {
            KTable<String, GenericRecord> keySetKTable = kTable.toStream().map((key, value) -> KeyValue.pair(SetKey(value, commonKey), value)).toTable();
            keySharedKTables.add(keySetKTable);
        }
        return keySharedKTables;
    }
    private static void SetupArguments(String[] args) {
        Arguments.Broker = args[0];
        Arguments.SchemaRegistryURL = args[1];
        Arguments.ApplicationID = "test-outer-join-5";
        Arguments.OutputTopic = "test-outer-join-5";
        Arguments.AutoOffsetResetConfig = args[3];
    }
}