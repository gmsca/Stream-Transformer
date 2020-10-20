package gms.cims.bridge;

import io.confluent.common.config.ConfigException;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaAvroSerializer extends AbstractKafkaAvroSerializer implements Serializer<Object> {

    @Override
    protected void configure(KafkaAvroSerializerConfig config) {
        this.autoRegisterSchema = true;
        try {
            final List<String> schema = Collections.singletonList(Arguments.SchemaRegistry);
            this.schemaRegistry = new CachedSchemaRegistryClient(schema, Integer.MAX_VALUE);
        } catch (ConfigException e) {
            throw new org.apache.kafka.common.config.ConfigException(e.getMessage());
        }
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        configure(null);
    }

    @Override
    public byte[] serialize( String subject, final Object object) {
        return ConvertToBytes(SchemaRegistryName.Get(subject, this.valueSubjectNameStrategy), object);
    }

    @Override
    public void close() {
    }

    private byte[] ConvertToBytes(String subject, final Object o){

        Schema schema = null;
        if (o == null) {
            return null;
        } else {
            String restClientErrorMsg = "";
            try {
                schema = getSchema(o);
                int id;
                if (this.autoRegisterSchema) {
                    restClientErrorMsg = "Error registering Avro schema: ";
                    id = this.schemaRegistry.register(subject, schema);
                } else {
                    restClientErrorMsg = "Error retrieving Avro schema: ";
                    id = this.schemaRegistry.getId(subject, schema);
                }

                ByteArrayOutputStream out = new ByteArrayOutputStream();
                out.write(0);
                out.write(ByteBuffer.allocate(4).putInt(id).array());
                if (o instanceof byte[]) {
                    out.write((byte[])((byte[])o));
                } else {
                    BinaryEncoder encoder = this.encoderFactory.directBinaryEncoder(out, (BinaryEncoder)null);
                    Object value = o instanceof NonRecordContainer ? ((NonRecordContainer)o).getValue() : o;
                    Object writer;
                    if (value instanceof SpecificRecord) {
                        writer = new SpecificDatumWriter(schema);
                    } else {
                        writer = new GenericDatumWriter<>(schema);
                    }

                    ((DatumWriter)writer).write(value, encoder);
                    encoder.flush();
                }

                byte[] bytes = out.toByteArray();
                out.close();
                return bytes;
            } catch (RuntimeException | IOException var10) {
                throw new SerializationException("Error serializing Avro message", var10);
            } catch (RestClientException var11) {
                throw new SerializationException(restClientErrorMsg + schema, var11);
            }
        }
    }

    private static Schema getSchema(Object object) {
        if (object == null) {
            return (Schema)primitiveSchemas.get("Null");
        } else if (object instanceof Boolean) {
            return (Schema)primitiveSchemas.get("Boolean");
        } else if (object instanceof Integer) {
            return (Schema)primitiveSchemas.get("Integer");
        } else if (object instanceof Long) {
            return (Schema)primitiveSchemas.get("Long");
        } else if (object instanceof Float) {
            return (Schema)primitiveSchemas.get("Float");
        } else if (object instanceof Double) {
            return (Schema)primitiveSchemas.get("Double");
        } else if (object instanceof CharSequence) {
            return (Schema)primitiveSchemas.get("String");
        } else if (object instanceof byte[]) {
            return (Schema)primitiveSchemas.get("Bytes");
        } else if (object instanceof GenericContainer) {
            return ((GenericContainer)object).getSchema();
        } else {
            throw new IllegalArgumentException("Unsupported Avro type. Supported types are null, Boolean, Integer, Long, Float, Double, String, byte[] and IndexedRecord");
        }
    }

    private static final Map<String, Schema> primitiveSchemas;

    static {
        Schema.Parser parser = new Schema.Parser();
        parser.setValidateDefaults(false);
        primitiveSchemas = new HashMap<>();
        primitiveSchemas.put("Null", createPrimitiveSchema(parser, "null"));
        primitiveSchemas.put("Boolean", createPrimitiveSchema(parser, "boolean"));
        primitiveSchemas.put("Integer", createPrimitiveSchema(parser, "int"));
        primitiveSchemas.put("Long", createPrimitiveSchema(parser, "long"));
        primitiveSchemas.put("Float", createPrimitiveSchema(parser, "float"));
        primitiveSchemas.put("Double", createPrimitiveSchema(parser, "double"));
        primitiveSchemas.put("String", createPrimitiveSchema(parser, "string"));
        primitiveSchemas.put("Bytes", createPrimitiveSchema(parser, "bytes"));
    }
    private static Schema createPrimitiveSchema(Schema.Parser parser, String type) {
        String schemaString = String.format("{\"type\" : \"%s\"}", type);
        return parser.parse(schemaString);
    }

    private final EncoderFactory encoderFactory = EncoderFactory.get();

    protected static Map<String, Schema> getPrimitiveSchemas() {
        return Collections.unmodifiableMap(primitiveSchemas);
    }
}
