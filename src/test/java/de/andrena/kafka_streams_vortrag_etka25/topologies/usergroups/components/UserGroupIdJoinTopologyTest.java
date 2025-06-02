package de.andrena.kafka_streams_vortrag_etka25.topologies.usergroups.components;

import de.andrena.kafka_streams_vortrag_etka25.model.User;
import de.andrena.kafka_streams_vortrag_etka25.model.UserMapper;
import de.andrena.kafka_streams_vortrag_etka25.topologies.AbstractTopologyUnitTest;
import de.andrena.kafka_streams_vortrag_etka25.util.JsonMapper;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class UserGroupIdJoinTopologyTest extends AbstractTopologyUnitTest {

    public static final String USERS_INPUT_TOPIC = "users";
    public static final String USER_TO_GROUP_MAPPING_INPUT_TOPIC = "user-to-group-mapping";
    public static final String USERS_WITH_GROUP_IDS_OUTPUT_TOPIC = "users-with-group-ids";
    private TestInputTopic<String, String> usersInputTopic;
    private TestInputTopic<String, String> userToGroupMappingInputTopic;
    private TestOutputTopic<String, String> outputTopic;

    private final UserMapper userMapper = new UserMapper(new JsonMapper());
    private final UserGroupIdJoinTopology userGroupIdJoinTopology = new UserGroupIdJoinTopology(userMapper);

    @Override
    protected Topology createTopology(StreamsBuilder streamsBuilder) {
        KStream<String, String> users = topicToStream(USERS_INPUT_TOPIC);
        KStream<String, String> userToGroupMapping = topicToStream(USER_TO_GROUP_MAPPING_INPUT_TOPIC);
        KTable<String, String> result = userGroupIdJoinTopology.build(users, userToGroupMapping);
        tableToTopic(result, USERS_WITH_GROUP_IDS_OUTPUT_TOPIC);

        return streamsBuilder.build();
    }

    @Override
    protected void createTestTopics() {
        usersInputTopic = testInputTopic(USERS_INPUT_TOPIC);
        userToGroupMappingInputTopic = testInputTopic(USER_TO_GROUP_MAPPING_INPUT_TOPIC);
        outputTopic = testOutputTopic(USERS_WITH_GROUP_IDS_OUTPUT_TOPIC);
    }

    @Test
    public void joinsUserWithGroupId_whenBothRecordsArePresent() {
        User user1 = new User("user1", "Alice", null);

        usersInputTopic.pipeInput("user1", userMapper.serialize(user1));
        userToGroupMappingInputTopic.pipeInput("user1", "group1");

        var outputRecords = getOutputRecords();
        assertThat(outputRecords).hasSize(1);
        assertThat(outputRecords.get(0).key).isEqualTo("user1");
        assertThat(outputRecords.get(0).value.group()).isEqualTo("group1");
    }

    @Test
    public void removeUserFromOutput_whenUserIsRemovedFromGroup() {
        User user = new User("user1", "Alice", null);

        usersInputTopic.pipeInput("user1", userMapper.serialize(user));
        userToGroupMappingInputTopic.pipeInput("user1", "group1");
        userToGroupMappingInputTopic.pipeInput("user1", (String) null); // Tombstone

        var outputRecords = getOutputRecords();
        assertThat(outputRecords).hasSize(2);

        assertThat(outputRecords.get(0).key).isEqualTo("user1");
        assertThat(outputRecords.get(0).value.group()).isEqualTo("group1");

        assertThat(outputRecords.get(1).key).isEqualTo("user1");
        assertThat(outputRecords.get(1).value).isNull(); // Tombstone in output
    }

    private List<KeyValue<String, User>> getOutputRecords() {
        return outputTopic.readRecordsToList().stream()
                .map(record -> KeyValue.pair(record.key(), userMapper.deserialize(record.value())))
                .toList();
    }
}
