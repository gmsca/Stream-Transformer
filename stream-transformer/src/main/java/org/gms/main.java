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

    // define how to transform streams in build topology
    private static Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        /*
        create 1 KTable from array of input topics
        the first element in the arraylist is the left-most item and gets joined last
        specify a common key between all tables
        provide a class based on the final schema
        */
        KTable<String, GenericRecord> ClaimCase_CaseNoteLink = CreateJoinedKTable(
                new ArrayList(Arrays.asList(
                        "CIMSTEST.Financial.ClaimCase",
                        "CIMSTEST.Financial.CaseNoteLink"
                )),
                "CA_CaseID",
                ClaimCase_ClaimNoteLink.class,
                builder
        );

        KTable<String, GenericRecord> ClaimStatus_ClaimStatusClaimLink = CreateJoinedKTable(
                new ArrayList(Arrays.asList(
                        "CIMSTEST.Financial.ClaimStatusClaimLink",
                        "CIMSTEST.Financial.ClaimStatus"
                )),
                "CS_ClaimStatusID",
                ClaimStatus_ClaimStatusClaimLink.class,
                builder
        );

        KTable<String, GenericRecord> Claim_ClaimStatus_ClaimStatusClaimLink = CreateJoinedKTable(
                new ArrayList(Arrays.asList(
                    "CIMSTEST.Financial.Claim",
                    ClaimStatus_ClaimStatusClaimLink
                )),
                "CL_ClaimID",
                Claim_ClaimStatus_ClaimStatusClaimLink.class,
                builder
        );

        KTable<String, GenericRecord> Claim_ClaimStatus_ClaimStatusClaimLink_ClaimCase_CaseNoteLink = CreateJoinedKTable(
                new ArrayList(Arrays.asList(
                    Claim_ClaimStatus_ClaimStatusClaimLink,
                    ClaimCase_CaseNoteLink
                )),
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

    // return one KTable from Array of KTables that are left joined
    private static KTable<String, GenericRecord> CreateJoinedKTable(ArrayList<Object> topics, String commonKey, Class<?> Class, StreamsBuilder builder) {
        ArrayList<KTable<String, GenericRecord>> KeySetKTables = new ArrayList<>();
        for (Object topic : topics) {
            if (topic.getClass()==String.class) KeySetKTables.add(SetCommonKey(GetKTable(builder, topic.toString()), commonKey));
            if (topic.getClass()==KTableImpl.class) KeySetKTables.add(SetCommonKey((KTable<String, GenericRecord>) topic, commonKey));
        }

        return LeftJoinKTables(KeySetKTables, Class);
    }

    // left join Array of KTables
    private static KTable<String, GenericRecord> LeftJoinKTables(ArrayList<KTable<String, GenericRecord>> kTables, Class<?> Class) {
        KTable<String, GenericRecord> joined = kTables.get(kTables.size()-1);
        for (Integer i = kTables.size()-2; i >= 0; i--) {
            joined = kTables.get(i).leftJoin(joined, (left, right) -> {
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
        Arguments.ApplicationID = args[2];
        Arguments.OutputTopic = args[2];
        Arguments.AutoOffsetResetConfig = args[3];
    }
}