package de.andrena.kafka_streams_vortrag_etka25.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class JsonMapper {
    private final ObjectMapper objMapper = new ObjectMapper();

    public <T> T deserialize(String jsonString, Class<T> targetClass) {
        try {
            return objMapper.readValue(jsonString, targetClass);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> List<T> deserializeList(String jsonString, TypeReference<List<T>> valueTypeRef) {
        try {
            return objMapper.readValue(jsonString, valueTypeRef);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public String serialize(Object object) {
        try {
            return objMapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
