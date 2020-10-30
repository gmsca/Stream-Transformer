package org.gms;

import org.apache.kafka.streams.StreamsBuilder;

public class Arguments {
    public static String Broker;
    public static String SCHEMA_REGISTRY = "schema.registry.url";
    public static String SchemaRegistryURL;
    public static String ApplicationID;
    public static String OutputTopic;
    public static String AutoOffsetResetConfig;
    public static StreamsBuilder builder = new StreamsBuilder();;

}
