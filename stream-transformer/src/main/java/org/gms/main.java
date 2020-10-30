package org.gms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.internals.*;
import org.gms.claimclasses.*;
import org.json.JSONObject;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

        /*
        create 1 KTable from array of input topics
        the first element in the arraylist is the left-most item and gets joined last
        specify a regex expression that will select a common key between input tables
        provide a class based on the output schema
        */
        Object ClaimCase_CaseNoteLink = LeftJoinTopics(
                new ArrayList(Arrays.asList(
                        GetKTable("CIMSTEST.Financial.ClaimCase"),
                        GetKTable("CIMSTEST.Financial.CaseNoteLink")
                )),
                "CA_CaseID",
                ClaimCase_ClaimNoteLink.class
        );

        Object ClaimStatus_ClaimStatusClaimLink = LeftJoinTopics(
                new ArrayList(Arrays.asList(
                        GetKTable("CIMSTEST.Financial.ClaimStatusClaimLink"),
                        GetKTable("CIMSTEST.Financial.ClaimStatus")
                )),
                "CS_ClaimStatusID",
                ClaimStatus_ClaimStatusClaimLink.class
        );

        Object ClaimContractLink_ClaimContractRelationships = LeftJoinTopics(
                new ArrayList(Arrays.asList(
                        GetKStream("CIMSTEST.Financial.ClaimContractLink"),
                        GetKTable("CIMSTEST.Reference.ClaimContractRelationships")
                )),
                "CC_Relationship[ID]{0,2}",
                ClaimContractLink_ClaimContractRelationships.class
        );

        Object Claim_ClaimStatus_ClaimStatusClaimLink = LeftJoinTopics(
                new ArrayList(Arrays.asList(
                        GetKTable("CIMSTEST.Financial.Claim"),
                        ClaimStatus_ClaimStatusClaimLink
                )),
                "CL_ClaimID",
                Claim_ClaimStatus_ClaimStatusClaimLink.class
        );

        Object Claim_ClaimStatus_ClaimStatusClaimLink_ClaimCase_CaseNoteLink = LeftJoinTopics(
                new ArrayList(Arrays.asList(
                        Claim_ClaimStatus_ClaimStatusClaimLink,
                        ClaimCase_CaseNoteLink
                )),
                "CA_CaseID",
                Claim_ClaimStatus_ClaimStatusClaimLink_ClaimCase_ClaimNoteLink.class
        );

        //Claim_ClaimStatus_ClaimStatusClaimLink_ClaimCase_CaseNoteLink.toStream().to(Arguments.OutputTopic);

        /*String regex = "CC_Relationship[ID]{0,2}";
        KStream<String, GenericRecord> ClaimContractLink = builder.stream("CIMSTEST.Financial.ClaimContractLink");
        KStream<String, GenericRecord> ClaimContractRelationships = builder.stream("CIMSTEST.Reference.ClaimContractRelationships");
        KStream<String, GenericRecord> ClaimContractLink_KeySet = ClaimContractLink.map((key, value) -> KeyValue.pair(GetKey(value, regex), value));
        KTable<String, GenericRecord> ClaimContractRelationships_KeySet = ClaimContractRelationships.map((key, value) -> KeyValue.pair(GetKey(value, regex), value)).toTable();

        KStream<String, GenericRecord> joined = ClaimContractLink_KeySet.leftJoin(ClaimContractRelationships_KeySet, (left, right) -> {
            try {
                return MergeMessages(left, right, ClaimContractLink_ClaimContractRelationships.class);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                return null;
            }
        });

        joined.to("lookup-test");*/

        return Arguments.builder.build();
    }

    // return one KTable from Array of KTables that are left joined
    private static Object LeftJoinTopics(ArrayList<Object> topics, String regex, Class<?> Class) {
        ArrayList<Object> KeySetTopics = new ArrayList<>();
        for (Object topic : topics) KeySetTopics.add(SetCommonKey(topic, regex));
        return LeftJoin(KeySetTopics, Class);
    }

    // set the key of the KTable
    private static Object SetCommonKey(Object topic, String regex) {
        if (topic.getClass()==KTableImpl.class) return ((KTable<String, GenericRecord>) topic).toStream().map((key, value) -> KeyValue.pair(GetKey(value, regex), value)).toTable();
        if (topic.getClass()==KStreamImpl.class) return ((KStream<String, GenericRecord>) topic).map((key, value) -> KeyValue.pair(GetKey(value, regex), value));
        else return null;
    }

    // get the key of a genericRecord
    private static String GetKey(GenericRecord value, String regex) {
        if (value==null) return null;
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(value.toString());
        if (m.find()) return value.get(m.group(0)).toString();
        else return null;
    }

    // return KTable from topic name
    private static KTable<String, GenericRecord> GetKTable(String topic) {
        return Arguments.builder.table(topic);
    }

    // return KStream from topic name
    private static KStream<String, GenericRecord> GetKStream(String topic) {
        return Arguments.builder.stream(topic);
    }

    // left join Array of KTables
    private static Object LeftJoin(ArrayList<Object> topics, Class<?> Class) {
        Object joined = topics.get(topics.size()-1);
        for (Integer i = topics.size()-2; i >= 0; i--) {

            joined = topics.get(i).leftJoin(joined, (left, right) -> {
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

    // setup arguments for application
    private static void SetupArguments(String[] args) {
        Arguments.Broker = args[0];
        Arguments.SchemaRegistryURL = args[1];
        Arguments.ApplicationID = args[2];
        Arguments.OutputTopic = args[2];
        Arguments.AutoOffsetResetConfig = args[3];
    }
}