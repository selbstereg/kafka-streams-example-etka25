package de.andrena.kafka_streams_vortrag_etka25.topologies.usergroups.components;

import de.andrena.kafka_streams_vortrag_etka25.model.User;
import de.andrena.kafka_streams_vortrag_etka25.model.UserMapper;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class UserGroupIdJoinTopology {
    private final UserMapper userMapper;

    public KTable<String, String> build(KStream<String, String> users, KStream<String, String> userToGroupMapping) {
        KTable<String, String> usersTable = users.toTable(
                Materialized.as("users-table") // for changelog topic
        );

        KTable<String, String> userToGroupMappingTable = userToGroupMapping.toTable(
                // Materialized.as("user-to-group-mapping-table") // for changelog topic
        );

        return usersTable.join(
                userToGroupMappingTable,
                (userJson, groupId) -> {
                    User user = userMapper.deserialize(userJson);
                    User userWithGroup = new User(
                            user.id(),
                            user.name(),
                            groupId
                    );
                    return userMapper.serialize(userWithGroup);
                },
                Named.as("join-user-and-group-id"),
                Materialized.as("join-user-and-group-id")
        );
    }
}
