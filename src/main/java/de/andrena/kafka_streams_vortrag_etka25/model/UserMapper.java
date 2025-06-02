package de.andrena.kafka_streams_vortrag_etka25.model;

import com.fasterxml.jackson.core.type.TypeReference;
import de.andrena.kafka_streams_vortrag_etka25.util.JsonMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class UserMapper {
    private final JsonMapper jsonMapper;

    public User deserialize(String userJson) {
        return userJson == null ?
                null :
                jsonMapper.deserialize(userJson, User.class);
    }

    public List<User> deserializeGroup(String userGroupJson) {
        return userGroupJson == null ?
                null :
                jsonMapper.deserializeList(userGroupJson, new TypeReference<List<User>>() {});
    }

    public List<String> deserializeStringList(String stringListJson) {
        return stringListJson == null ?
                null :
                jsonMapper.deserializeList(stringListJson, new TypeReference<List<String>>() {});
    }

    public String serialize(Object obj) {
        return obj == null ? null : jsonMapper.serialize(obj);
    }
}
