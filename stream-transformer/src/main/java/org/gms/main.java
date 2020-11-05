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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@JsonIgnoreProperties(ignoreUnknown = true)

public class main {

    // main is entrypoint
    public static void main(String[] args) {
        SetupArguments(args);
        Topology topology = buildTopology();
        TopologyDescription top = topology.describe();
        System.out.println(top);
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

        KStream<String, GenericRecord> ClaimCase = GetKStream("CIMS.Financial.CaseNoteLink");
        KStream<String, GenericRecord> CaseNoteLink = GetKStream("CIMS.Financial.ClaimCase");
        KTable<String, GenericRecord> ClaimCase_set = ClaimCase.map((key, value) -> KeyValue.pair(GetKey(value, "CA_CaseID"), value)).toTable();
        KTable<String, GenericRecord> CaseNoteLink_set = CaseNoteLink.map((key, value) -> KeyValue.pair(GetKey(value, "CA_CaseID"), value)).toTable();
        KTable<String, GenericRecord> ClaimCase_CaseNoteLink =  ClaimCase_set.leftJoin(CaseNoteLink_set, (left, right) -> MergeMessages(left, right, ClaimCase_ClaimNoteLink.class));

        KStream<String, GenericRecord> ClaimStatus = GetKStream("CIMS.Financial.ClaimStatus");
        KStream<String, GenericRecord> ClaimStatusClaimLink = GetKStream("CIMS.Financial.ClaimStatusClaimLink");
        KTable<String, GenericRecord> ClaimStatus_set = ClaimStatus.map((key, value) -> KeyValue.pair(GetKey(value, "CS_ClaimStatusID"), value)).toTable();
        KTable<String, GenericRecord> ClaimStatusClaimLink_set = ClaimStatusClaimLink.map((key, value) -> KeyValue.pair(GetKey(value, "CS_ClaimStatusID"), value)).toTable();
        KTable<String, GenericRecord> ClaimStatus_ClaimStatusClaimLink =  ClaimStatus_set.leftJoin(ClaimStatusClaimLink_set, (left, right) -> MergeMessages(left, right, ClaimStatus_ClaimStatusClaimLink.class));

        KStream<String, GenericRecord> ClaimContractLink = GetKStream("CIMS.Financial.ClaimContractLink");
        KStream<String, GenericRecord> ClaimContractRelationships = GetKStream("CIMS.Reference.ClaimContractRelationships");
        KStream<String, GenericRecord> ClaimContractLink_set = ClaimContractLink.map((key, value) -> KeyValue.pair(GetKey(value, "CC_Relationship"), value));
        KTable<String, GenericRecord> ClaimContractRelationships_set = ClaimContractRelationships.map((key, value) -> KeyValue.pair(GetKey(value, "CC_RelationshipID"), value)).toTable();
        KStream<String, GenericRecord> ClaimContractLink_ClaimContractRelationships =  ClaimContractLink_set.leftJoin(ClaimContractRelationships_set, (left, right) -> MergeMessages(left, right, ClaimContractLink_ClaimContractRelationships.class));

        KStream<String, GenericRecord> Claim = GetKStream("CIMS.Financial.Claim");
        KTable<String, GenericRecord> Claim_set = Claim.map((key, value) -> KeyValue.pair(GetKey(value, "CL_ClaimID"), value)).toTable();
        KTable<String, GenericRecord> ClaimStatus_ClaimStatusClaimLink_set = ClaimStatus_ClaimStatusClaimLink.toStream().map((key, value) -> KeyValue.pair(GetKey(value, "CL_ClaimID"), value)).toTable();
        KTable<String, GenericRecord> Claim_ClaimStatus_ClaimStatusClaimLink =  Claim_set.leftJoin(ClaimStatus_ClaimStatusClaimLink_set, (left, right) -> MergeMessages(left, right, Claim_ClaimStatus_ClaimStatusClaimLink.class));

        KTable<String, GenericRecord> Claim_ClaimStatus_ClaimStatusClaimLink_set = Claim_ClaimStatus_ClaimStatusClaimLink.toStream().map((key, value) -> KeyValue.pair(GetKey(value, "CA_CaseID"), value)).toTable();
        KTable<String, GenericRecord> ClaimCase_CaseNoteLink_set = ClaimCase_CaseNoteLink.toStream().map((key, value) -> KeyValue.pair(GetKey(value, "CA_CaseID"), value)).toTable();
        KTable<String, GenericRecord> Claim_ClaimStatus_ClaimStatusClaimLink_ClaimCase_CaseNoteLink =  Claim_ClaimStatus_ClaimStatusClaimLink_set.leftJoin(ClaimCase_CaseNoteLink_set, (left, right) -> MergeMessages(left, right, Claim_ClaimStatus_ClaimStatusClaimLink_ClaimCase_ClaimNoteLink.class));

        KTable<String, GenericRecord> Claim_ClaimStatus_ClaimStatusClaimLink_ClaimCase_CaseNoteLink_set = Claim_ClaimStatus_ClaimStatusClaimLink_ClaimCase_CaseNoteLink.toStream().map((key, value) -> KeyValue.pair(GetKey(value, "CL_ClaimID"), value)).toTable();
        KTable<String, GenericRecord> ClaimContractLink_ClaimContractRelationships_set = ClaimContractLink_ClaimContractRelationships.map((key, value) -> KeyValue.pair(GetKey(value, "CL_ClaimID"), value)).toTable();
        KTable<String, GenericRecord> Claim_ClaimStatus_ClaimStatusClaimLink_ClaimCase_ClaimNoteLink_ClaimContractLink_ClaimContractRelationships =  Claim_ClaimStatus_ClaimStatusClaimLink_ClaimCase_CaseNoteLink_set.leftJoin(ClaimContractLink_ClaimContractRelationships_set, (left, right) -> MergeMessages(left, right, Claim_ClaimStatus_ClaimStatusClaimLink_ClaimCase_ClaimNoteLink_ClaimContractLink_ClaimContractRelationships.class));

        Claim_ClaimStatus_ClaimStatusClaimLink_ClaimCase_ClaimNoteLink_ClaimContractLink_ClaimContractRelationships.toStream().to(Arguments.OutputTopic);

        return Arguments.builder.build();
    }

    // get the key of a genericRecord
    private static String GetKey(GenericRecord value, String regex) {
        if (value==null) return null;
        /*Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(value.toString());
        if (m.find()) return value.get(m.group(0)).toString();*/
        else return value.get(regex).toString();
    }

    // return KTable from topic name
    private static KTable<String, GenericRecord> GetKTable(String topic) {
        return Arguments.builder.table(topic);
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