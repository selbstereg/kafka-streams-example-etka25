package de.andrena.kafka_streams_vortrag_etka25.topologies.usergroups.components;

import de.andrena.kafka_streams_vortrag_etka25.model.User;
import de.andrena.kafka_streams_vortrag_etka25.model.UserMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class GroupAggregationTopology {
    private final UserMapper userMapper;


    public KTable<String, String> build(KTable<String, String> usersWithGroupIds) {
        return usersWithGroupIds
                .groupBy(
                        (key, userJson) -> {
                            User user = userMapper.deserialize(userJson);
                            return KeyValue.pair(user.group(), userJson);
                        }
                )
                .aggregate(
                        () -> "[]",
                        (key, userJson, aggregate) -> {
                            List<User> userGroup = userMapper.deserializeGroup(aggregate);
                            User user = userMapper.deserialize(userJson);
                            userGroup.add(user);
                            return userMapper.serialize(userGroup);
                        },
                        (key, userJson, aggregate) -> {
                            List<User> userGroup = userMapper.deserializeGroup(aggregate);
                            User user = userMapper.deserialize(userJson);
                            userGroup.remove(user);
                            if (userGroup.isEmpty())
                                return null; // treat empty group as deleted => emit tombstone
                            else
                                return userMapper.serialize(userGroup);
                        }/*,
                        Materialized.as("aggregate-user-by-group")*/
                );
    }
}
