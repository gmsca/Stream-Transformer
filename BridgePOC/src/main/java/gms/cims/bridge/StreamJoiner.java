package gms.cims.bridge;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

public class StreamJoiner {
    public void Start() {
        Topology topology = buildTopology();
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, Arguments.outputTopic+"_id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Arguments.Broker);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put("schema.registry.url", Arguments.SchemaRegistry);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);

        final KafkaStreams streams = new KafkaStreams(topology, props);

        streams.cleanUp();
        streams.start();
        System.out.println(streams.toString());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private Topology buildTopology() {
        //topic2.print(Printed.toSysOut());
        //[ClaimStatus: {"CS_ClaimStatusID": 12288}, {"CS_ClaimStatusID": 12288, "CS_Description": "Claim contains invalid drugs                                                                        ", "__deleted": "false"}
        //ClaimStatusClaimLink: {"CL_ClaimID": 12229719, "CS_ClaimStatusID": 26711}, {"CL_ClaimID": 12229719, "CS_ClaimStatusID": 26711, "__deleted": "false"}
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, GenericRecord> ClaimStatus = builder.stream("CIMSTEST.Financial.ClaimStatus");
        KStream<String, GenericRecord> ClaimStatusClaimLink = builder.stream("CIMSTEST.Financial.ClaimStatusClaimLink");
        KStream<String, GenericRecord> ClaimContractLink = builder.stream("CIMSTEST.Financial.ClaimContractLink");

        KTable<String, GenericRecord> T_ClaimStatus = ClaimStatus.map((key, value) -> KeyValue.pair((value.get("CS_ClaimStatusID").toString()), value)).toTable();
        KTable<String, GenericRecord> T_ClaimStatusClaimLink = ClaimStatusClaimLink.map((key, value) -> KeyValue.pair((value.get("CS_ClaimStatusID").toString()), value)).toTable();
        KTable<String, GenericRecord> T_ClaimContractLink = ClaimContractLink.map((key, value) -> KeyValue.pair((value.get("CL_ClaimID").toString()), value)).toTable();

        KTable<String, GenericRecord> T_ClaimStatus_ClaimStatusClaimLink= innerJoinKTable(T_ClaimStatus, T_ClaimStatusClaimLink, ClaimStatus_ClaimStatusClaimLink.class);
        KTable<String, GenericRecord> T_ClaimStatus_ClaimStatusClaimLink_ClaimContractLink= innerJoinKTable(T_ClaimContractLink, T_ClaimStatus_ClaimStatusClaimLink, ClaimStatus_ClaimStatusClaimLink_ClaimContractLink.class);


        T_ClaimStatus_ClaimStatusClaimLink_ClaimContractLink.toStream().to(Arguments.outputTopic);

        return builder.build();
    }


    private KTable<String, GenericRecord> innerJoinKTable(KTable<String, GenericRecord> first, KTable<String, GenericRecord> second, Class<?> claimClass) {

      return first.join(second,
                (left,right) -> {
                    JSONObject leftJSON = new JSONObject(left.toString());
                    JSONObject rightJSON = new JSONObject(right.toString());
                    ObjectMapper objectMapper = new ObjectMapper();

                    leftJSON.keys().forEachRemaining(k -> {
                        if (!rightJSON.has(k)) {
                            rightJSON.put(k, leftJSON.get(k));
                        }
                    });

                    try {
                        return (GenericRecord) objectMapper.readValue(rightJSON.toString(), claimClass);
                    } catch (IOException e) {
                        e.printStackTrace();
                        return null;
                    }
                }
        );
    }
}