package gms.cims.bridge;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KafkaAvroSerde implements Serde<Object> {

    private KafkaAvroSerializer serializer = new KafkaAvroSerializer();
    private KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }

    @Override
    public Serializer<Object> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Object> deserializer() {
        return deserializer;
    }
}
