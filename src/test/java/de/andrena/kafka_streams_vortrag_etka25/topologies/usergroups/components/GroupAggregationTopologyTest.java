package de.andrena.kafka_streams_vortrag_etka25.topologies.usergroups.components;

import de.andrena.kafka_streams_vortrag_etka25.model.User;
import de.andrena.kafka_streams_vortrag_etka25.model.UserMapper;
import de.andrena.kafka_streams_vortrag_etka25.topologies.AbstractTopologyUnitTest;
import de.andrena.kafka_streams_vortrag_etka25.util.JsonMapper;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class GroupAggregationTopologyTest extends AbstractTopologyUnitTest {
    public static final String USERS_WITH_GROUP_IDS_INPUT_TOPIC = "users-with-group-ids";
    public static final String USERS_GROUPS_OUTPUT_TOPIC = "user-groups";
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;

    private final UserMapper userMapper = new UserMapper(new JsonMapper());
    private final GroupAggregationTopology groupAggregationTopology = new GroupAggregationTopology(userMapper);

    @Override
    protected Topology createTopology(StreamsBuilder streamsBuilder) {
        KTable<String, String> usersWithGroupIds = topicToTable(USERS_WITH_GROUP_IDS_INPUT_TOPIC);

        KTable<String, String> userGroups = groupAggregationTopology.build(usersWithGroupIds);

        tableToTopic(userGroups, USERS_GROUPS_OUTPUT_TOPIC);

        return streamsBuilder.build();
    }

    @Override
    protected void createTestTopics() {
        inputTopic = testInputTopic(USERS_WITH_GROUP_IDS_INPUT_TOPIC);
        outputTopic = testOutputTopic(USERS_GROUPS_OUTPUT_TOPIC);
    }

    @Test
    public void addsUsersToList_whenGroupIdIsEqual() {
        User user1 = user("user1", "grp1");
        User user2 = user("user2", "grp1");

        inputTopic.pipeInput("user1", userMapper.serialize(user1));

        var record = deserialize(outputTopic.readRecord());
        assertThat(record).isEqualTo(KeyValue.pair("grp1", List.of(user1)));


        inputTopic.pipeInput("user2", userMapper.serialize(user2));

        record = deserialize(outputTopic.readRecord());
        assertThat(record).isEqualTo(KeyValue.pair("grp1", List.of(user1, user2)));
    }

    @Test
    public void addsUsersToSeparateLists_whenGroupIdIsDifferent() {
        User user1 = user("user1", "grp1");
        User user2 = user("user2", "grp2");

        inputTopic.pipeInput("user1", userMapper.serialize(user1));
        inputTopic.pipeInput("user2", userMapper.serialize(user2));

        var records = getOutputRecords();

        assertThat(records).containsExactly(
                KeyValue.pair("grp1", List.of(user1)),
                KeyValue.pair("grp2", List.of(user2))
        );
    }


    @Test
    public void addsUserFromList_whenGroupChanges() {
        User user1 = user("user1", "grp1");
        User user1Updated = user("user1", "grp2");

        inputTopic.pipeInput("user1", userMapper.serialize(user1));

        var records = getOutputRecords();

        assertThat(records).containsExactly(
            KeyValue.pair("grp1", List.of(user1))
        );

        inputTopic.pipeInput("user1", userMapper.serialize(user1Updated));

        records = getOutputRecords();
        assertThat(records).containsExactly(
                KeyValue.pair("grp1", null),
                KeyValue.pair("grp2", List.of(user1Updated))
        );
    }

    private List<KeyValue<String, List<User>>> getOutputRecords() {
        return outputTopic.readRecordsToList().stream()
                .map(this::deserialize)
                .toList();
    }

    private KeyValue<String, List<User>> deserialize(TestRecord<String, String> record) {
        return KeyValue.pair(record.key(), userMapper.deserializeGroup(record.value()));
    }

    private static User user(String id, String group) {
        return new User(id, "foo", group);
    }
}
