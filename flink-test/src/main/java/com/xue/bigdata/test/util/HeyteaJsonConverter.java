package com.xue.bigdata.test.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.apache.kafka.common.utils.Utils.mkSet;

/**
 * @author: mingway
 * @date: 2021/12/22 4:52 下午
 */
public class HeyteaJsonConverter implements Converter, HeaderConverter {

    private static final JsonNodeFactory JSON_NODE_FACTORY = JsonNodeFactory.withExactBigDecimals(true);

    private final JsonSerializer serializer;

    private final SimpleDateFormat timestampFmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");

    public HeyteaJsonConverter() {
        serializer = new JsonSerializer(
                mkSet(),
                JSON_NODE_FACTORY
        );
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (schema == null && value == null) {
            return null;
        }

        JsonNode jsonValue = convertToJsonWithoutEnvelope(schema, value);
        try {
            return serializer.serialize(topic, jsonValue);
        } catch (SerializationException e) {
            throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e);
        }
    }

    private JsonNode convertToJsonWithoutEnvelope(Schema schema, Object value) {
        return convertToJson(schema, value);
    }

    private JsonNode convertToJson(Schema schema, Object value) {
        if (value == null) {
            if (schema == null) // Any schema is valid and we don't have a default, so treat this as an optional schema
                return null;
            if (schema.defaultValue() != null)
                return convertToJson(schema, schema.defaultValue());
            if (schema.isOptional())
                return JSON_NODE_FACTORY.nullNode();
            throw new DataException("Conversion error: null value for field that is required and has no default value");
        }

        try {
            final Schema.Type schemaType;
            if (schema == null) {
                schemaType = ConnectSchema.schemaType(value.getClass());
                if (schemaType == null)
                    throw new DataException("Java class " + value.getClass() + " does not have corresponding schema type.");
            } else {
                schemaType = schema.type();
            }
            switch (schemaType) {
                case INT8:
                    return JSON_NODE_FACTORY.numberNode((Byte) value);
                case INT16:
                    return JSON_NODE_FACTORY.numberNode((Short) value);
                case INT32:
                    if ("io.debezium.time.Date".equals(schema.name())) {
                        return JSON_NODE_FACTORY.textNode(LocalDate.ofEpochDay((Integer) value).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
                    } else if ("io.debezium.time.Timestamp".equals(schema.name())) {
                        if (Objects.isNull(value)) {
                            return JSON_NODE_FACTORY.numberNode((Long) value);
                        }
                        Long cur = LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) value), TimeZone.getTimeZone("UTC").toZoneId()).toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
                        return JSON_NODE_FACTORY.numberNode(cur);
                    }  else {
                        return JSON_NODE_FACTORY.numberNode((Integer) value);
                    }
                case INT64:
                    if ("io.debezium.time.Date".equals(schema.name())) {
                        return JSON_NODE_FACTORY.textNode(LocalDate.ofEpochDay((Integer) value).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
                    } else if ("io.debezium.time.Timestamp".equals(schema.name())) {
                        if (Objects.isNull(value)) {
                            return JSON_NODE_FACTORY.numberNode((Long) value);
                        }
                        Long cur = LocalDateTime.ofInstant(Instant.ofEpochMilli((Long) value), TimeZone.getTimeZone("UTC").toZoneId()).toInstant(ZoneOffset.ofHours(8)).toEpochMilli();
                        return JSON_NODE_FACTORY.numberNode(cur);
                    } else {
                        return JSON_NODE_FACTORY.numberNode((Long) value);
                    }
                case FLOAT32:
                    return JSON_NODE_FACTORY.numberNode((Float) value);
                case FLOAT64:
                    return JSON_NODE_FACTORY.numberNode((Double) value);
                case BOOLEAN:
                    return JSON_NODE_FACTORY.booleanNode((Boolean) value);
                case STRING:
                    if ("io.debezium.time.ZonedTimestamp".equals(schema.name())) {
                        value = timestampFmt.parse((String) value).getTime();
                        return JSON_NODE_FACTORY.numberNode((Long) value);
                    } else {
                        CharSequence charSeq = (CharSequence) value;
                        return JSON_NODE_FACTORY.textNode(charSeq.toString());
                    }
                case BYTES:
                    if (value instanceof byte[])
                        return JSON_NODE_FACTORY.binaryNode((byte[]) value);
                    else if (value instanceof ByteBuffer)
                        return JSON_NODE_FACTORY.binaryNode(((ByteBuffer) value).array());
                    else
                        throw new DataException("Invalid type for bytes type: " + value.getClass());
                case ARRAY: {
                    Collection collection = (Collection) value;
                    ArrayNode list = JSON_NODE_FACTORY.arrayNode();
                    for (Object elem : collection) {
                        Schema valueSchema = schema == null ? null : schema.valueSchema();
                        JsonNode fieldValue = convertToJson(valueSchema, elem);
                        list.add(fieldValue);
                    }
                    return list;
                }
                case MAP: {
                    Map<?, ?> map = (Map<?, ?>) value;
                    // If true, using string keys and JSON object; if false, using non-string keys and Array-encoding
                    boolean objectMode;
                    if (schema == null) {
                        objectMode = true;
                        for (Map.Entry<?, ?> entry : map.entrySet()) {
                            if (!(entry.getKey() instanceof String)) {
                                objectMode = false;
                                break;
                            }
                        }
                    } else {
                        objectMode = schema.keySchema().type() == Schema.Type.STRING;
                    }
                    ObjectNode obj = null;
                    ArrayNode list = null;
                    if (objectMode)
                        obj = JSON_NODE_FACTORY.objectNode();
                    else
                        list = JSON_NODE_FACTORY.arrayNode();
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                        Schema keySchema = schema == null ? null : schema.keySchema();
                        Schema valueSchema = schema == null ? null : schema.valueSchema();
                        JsonNode mapKey = convertToJson(keySchema, entry.getKey());
                        JsonNode mapValue = convertToJson(valueSchema, entry.getValue());

                        if (objectMode)
                            obj.set(mapKey.asText(), mapValue);
                        else
                            list.add(JSON_NODE_FACTORY.arrayNode().add(mapKey).add(mapValue));
                    }
                    return objectMode ? obj : list;
                }
                case STRUCT: {
                    Struct struct = (Struct) value;
                    if (!struct.schema().equals(schema))
                        throw new DataException("Mismatching schema.");
                    ObjectNode obj = JSON_NODE_FACTORY.objectNode();
                    for (Field field : schema.fields()) {
                        obj.set(field.name(), convertToJson(field.schema(), struct.get(field)));
                    }
                    return obj;
                }
            }

            throw new DataException("Couldn't convert " + value + " to JSON.");
        } catch (ClassCastException | ParseException e) {
            String schemaTypeStr = (schema != null) ? schema.type().toString() : "unknown schema";
            throw new DataException("Invalid type for " + schemaTypeStr + ": " + value.getClass());
        }
    }


    @Override
    public SchemaAndValue toConnectData(String s, byte[] bytes) {
        return null;
    }

    @Override
    public SchemaAndValue toConnectHeader(String s, String s1, byte[] bytes) {
        return null;
    }

    @Override
    public byte[] fromConnectHeader(String s, String s1, Schema schema, Object o) {
        return new byte[0];
    }

    @Override
    public ConfigDef config() {
        return null;
    }

    class JsonSerializer implements Serializer<JsonNode> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        /**
         * Default constructor needed by Kafka
         */
        public JsonSerializer() {
            this(Collections.emptySet(), JsonNodeFactory.withExactBigDecimals(true));
        }

        /**
         * A constructor that additionally specifies some {@link SerializationFeature}
         * for the serializer
         *
         * @param serializationFeatures the specified serialization features
         * @param jsonNodeFactory the json node factory to use.
         */
        JsonSerializer(
                final Set<SerializationFeature> serializationFeatures,
                final JsonNodeFactory jsonNodeFactory
        ) {
            serializationFeatures.forEach(objectMapper::enable);
            objectMapper.setNodeFactory(jsonNodeFactory);
        }

        @Override
        public byte[] serialize(String topic, JsonNode data) {
            if (data == null)
                return null;

            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new SerializationException("Error serializing JSON message", e);
            }
        }
    }
}