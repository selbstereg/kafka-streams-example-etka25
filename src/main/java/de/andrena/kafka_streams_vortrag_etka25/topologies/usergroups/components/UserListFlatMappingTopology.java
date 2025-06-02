package de.andrena.kafka_streams_vortrag_etka25.topologies.usergroups.components;

import de.andrena.kafka_streams_vortrag_etka25.model.UserMapper;
import de.andrena.kafka_streams_vortrag_etka25.processors.StatefulFlatMapStreamFactory;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class UserListFlatMappingTopology {
    private final StatefulFlatMapStreamFactory flatMapFactory;
    private final UserMapper userMapper;

    public KStream<String, String> build(KStream<String, String> userIdListStream) {
        return flatMapFactory.create(
                userIdListStream,
                (userGroupId, userList) -> {
                    List<String> userIds = userMapper.deserializeStringList(userList);
                    return userIds.stream()
                            .map(userId -> KeyValue.pair(userId, userGroupId));
                },
                "flat-map-user-id-list"
        );
    }
}
