package de.andrena.kafka_streams_vortrag_etka25.topologies.usergroups;

import de.andrena.kafka_streams_vortrag_etka25.topologies.usergroups.components.GroupAggregationTopology;
import de.andrena.kafka_streams_vortrag_etka25.topologies.usergroups.components.UserGroupIdJoinTopology;
import de.andrena.kafka_streams_vortrag_etka25.topologies.usergroups.components.UserListFlatMappingTopology;
import de.andrena.kafka_streams_vortrag_etka25.util.TopicConnector;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class UserGroupRootTopology {
    public static final String USER_LISTS_INPUT_TOPIC = "user-lists";
    public static final String USERS_INPUT_TOPIC = "users";
    public static final String USER_GROUPS_OUTPUT_TOPIC = "user-groups";

    private final TopicConnector topicConnector;
    private final UserListFlatMappingTopology userListFlatMappingTopology;
    private final UserGroupIdJoinTopology userGroupIdJoinTopology;
    private final GroupAggregationTopology groupAggregationTopology;

    @PostConstruct
    public void createUserGroupRootTopology() {
        // key: groupId, value: list of userIds
        KStream<String, String> userLists = topicConnector.readInputTopic(USER_LISTS_INPUT_TOPIC);
        KStream<String, String> users = topicConnector.readInputTopic(USERS_INPUT_TOPIC);

        // 1. flat map user group - key: userId, value: groupId
        KStream<String, String> userToGroupMapping = userListFlatMappingTopology.build(userLists);

        // 2. join users with userToGroupMapping to resolve their groups -
        // key: userId, value: user with group
        KTable<String, String> usersWithGroupIds = userGroupIdJoinTopology.build(users, userToGroupMapping);

        // 3. aggregate users
        // key: groupId, value: list of user objects
        KTable<String, String> userGroups = groupAggregationTopology.build(usersWithGroupIds);

        topicConnector.writeToTopic(userGroups, USER_GROUPS_OUTPUT_TOPIC);
    }
}
