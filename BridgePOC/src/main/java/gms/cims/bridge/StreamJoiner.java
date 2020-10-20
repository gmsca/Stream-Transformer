package gms.cims.bridge;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Properties;

public class StreamJoiner {
    GenericRecord genericRecord;
    public void Start() {
        Arguments.outputTopic="out4";
        Topology topology = buildTopology();
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, Arguments.outputTopic+"_id");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Arguments.Broker);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, KafkaAvroSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaAvroSerde.class);

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

        KStream<GenericRecord, GenericRecord> ClaimStatus = builder.stream("CIMSTEST.Financial.ClaimStatus");
        KStream<GenericRecord, GenericRecord> ClaimStatusClaimLink = builder.stream("CIMSTEST.Financial.ClaimStatusClaimLink");
        KStream<GenericRecord, GenericRecord> ClaimContractLink = builder.stream("CIMSTEST.Financial.ClaimContractLink");

        KTable<Integer, GenericRecord> T_ClaimStatus = ClaimStatus.map((key, value) -> KeyValue.pair(((Integer) (key.get("CS_ClaimStatusID"))), value)).toTable();
        KTable<Integer, GenericRecord> T_ClaimStatusClaimLink = ClaimStatusClaimLink.map((key, value) -> KeyValue.pair(((Integer) (key.get("CS_ClaimStatusID"))), value)).toTable();
        KTable<Integer, GenericRecord> T_ClaimContractLink = ClaimContractLink.map((key, value) -> KeyValue.pair(((Integer) (key.get("CL_ClaimID"))), value)).toTable();

        KTable<Integer, GenericRecord> T_ClaimStatus_ClaimStatusClaimLink= innerJoinKTable(T_ClaimStatusClaimLink,T_ClaimStatus, "ClaimStatus_ClaimStatusClaimLink");
        KTable<Integer, GenericRecord> T_ClaimStatus_ClaimStatusClaimLink_ClaimContractLink= innerJoinKTable(T_ClaimContractLink,T_ClaimStatus_ClaimStatusClaimLink, "ClaimStatus_ClaimStatusClaimLink_ClaimContractLink");

        T_ClaimStatus_ClaimStatusClaimLink_ClaimContractLink.toStream().to(Arguments.outputTopic);

        return builder.build();
    }


    private   KTable<Integer, GenericRecord> innerJoinKTable(KTable<Integer, GenericRecord> first, KTable<Integer, GenericRecord> second, String className) {

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
                        switch (className) {
                            case "ClaimStatus_ClaimStatusClaimLink":    genericRecord = objectMapper.readValue(rightJSON.toString(), ClaimStatus_ClaimStatusClaimLink.class);break;
                            case "ClaimStatus_ClaimStatusClaimLink_ClaimContractLink":    genericRecord = objectMapper.readValue(rightJSON.toString(), ClaimStatus_ClaimStatusClaimLink_ClaimContractLink.class);break;
                            default:  break;
                        }

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    return genericRecord;
                }
        );



    }
}