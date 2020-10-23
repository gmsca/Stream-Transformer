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
import org.apache.kafka.streams.kstream.internals.KTableImpl;

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
                "CIMSTEST.Financial.ClaimCase",
                "CIMSTEST.Financial.CaseNoteLink",
                "CA_CaseID",
                ClaimCase_ClaimNoteLink.class,
                builder
                );

        KTable<String, GenericRecord> ClaimStatus_ClaimStatusClaimLink = CreateJoinedKTable(
                "CIMSTEST.Financial.ClaimStatusClaimLink",
                "CIMSTEST.Financial.ClaimStatus",
                "CS_ClaimStatusID",
                ClaimStatus_ClaimStatusClaimLink.class,
                builder
        );

        KTable<String, GenericRecord> Claim_ClaimStatus_ClaimStatusClaimLink = CreateJoinedKTable(
                "CIMSTEST.Financial.Claim",
                ClaimStatus_ClaimStatusClaimLink,
                "CL_ClaimID",
                Claim_ClaimStatus_ClaimStatusClaimLink.class,
                builder
        );

        KTable<String, GenericRecord> Claim_ClaimStatus_ClaimStatusClaimLink_ClaimCase_CaseNoteLink = CreateJoinedKTable(
                Claim_ClaimStatus_ClaimStatusClaimLink,
                ClaimCase_CaseNoteLink,
                "CA_CaseID",
                Claim_ClaimStatus_ClaimStatusClaimLink_ClaimCase_ClaimNoteLink.class,
                builder
        );

        Claim_ClaimStatus_ClaimStatusClaimLink_ClaimCase_CaseNoteLink.toStream().to(Arguments.OutputTopic);

        return builder.build();
    }

    // return KTable from topic name
    private static KTable<String, GenericRecord> GetKTable(StreamsBuilder builder, String topic) {
        return builder.table(topic);
    }

    // return KTable of 2 KTables that are left joined
    private static KTable<String, GenericRecord> CreateJoinedKTable(Object leftTopic, Object rightTopic, String commonKey, Class<?> Class, StreamsBuilder builder) {

        if (leftTopic.getClass()==String.class && rightTopic.getClass()==String.class) {
            return LeftJoinKTables(
                    SetCommonKey(GetKTable(builder, leftTopic.toString()), commonKey),
                    SetCommonKey(GetKTable(builder, rightTopic.toString()), commonKey),
                    Class
            );
        }

        if (leftTopic.getClass()==KTableImpl.class && rightTopic.getClass()==String.class) {
            return LeftJoinKTables(
                    SetCommonKey((KTable<String, GenericRecord>) leftTopic, commonKey),
                    SetCommonKey(GetKTable(builder, rightTopic.toString()), commonKey),
                    Class
            );
        }

        if (leftTopic.getClass()==String.class && rightTopic.getClass()==KTableImpl.class) {
            return LeftJoinKTables(
                    SetCommonKey(GetKTable(builder, leftTopic.toString()), commonKey),
                    SetCommonKey((KTable<String, GenericRecord>) rightTopic, commonKey),
                    Class
            );
        }

        if (leftTopic.getClass()==KTableImpl.class && rightTopic.getClass()==KTableImpl.class) {
            return LeftJoinKTables(
                    SetCommonKey((KTable<String, GenericRecord>) leftTopic, commonKey),
                    SetCommonKey((KTable<String, GenericRecord>) rightTopic, commonKey),
                    Class
            );
        }
        return null;
    }

    // left join 2 KTables
    private static KTable<String, GenericRecord> LeftJoinKTables(KTable<String, GenericRecord> leftKTable, KTable<String, GenericRecord> rightKTable, Class<?> Class) {
        return leftKTable.leftJoin(rightKTable, (left, right) -> {
            try {
                return MergeMessages(left, right, Class);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                return null;
            }
        });
    }

    // merge 2 genericRecords and return a new genericRecord using the defined output class
    private static GenericRecord MergeMessages(GenericRecord left, GenericRecord right, Class<?> Class) throws JsonProcessingException {
        String mergedValues = MergeValues(left, right);
        ObjectMapper objectMapper = new ObjectMapper();
        return (GenericRecord) objectMapper.readValue(mergedValues, Class);
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

    // get the key of a genericRecord
    private static String GetKey(GenericRecord value, String commonKey) {
        if (value==null) return null;
        return value.get(commonKey).toString();
    }

    // set the key of the KTable
    private static KTable<String, GenericRecord> SetCommonKey(KTable<String, GenericRecord> kTable, String commonKey) {
        return kTable.toStream().map((key, value) -> KeyValue.pair(GetKey(value, commonKey), value)).toTable();
    }

    // setup arguments for application
    private static void SetupArguments(String[] args) {
        Arguments.Broker = args[0];
        Arguments.SchemaRegistryURL = args[1];
        Arguments.ApplicationID = "test-outer-join";
        Arguments.OutputTopic = "test-outer-join";
        Arguments.AutoOffsetResetConfig = args[3];
    }
}