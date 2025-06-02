package de.andrena.kafka_streams_vortrag_etka25.topologies.usergroups.components;

import de.andrena.kafka_streams_vortrag_etka25.model.UserMapper;
import de.andrena.kafka_streams_vortrag_etka25.processors.StatefulFlatMapStreamFactory;
import de.andrena.kafka_streams_vortrag_etka25.topologies.AbstractTopologyUnitTest;
import de.andrena.kafka_streams_vortrag_etka25.util.JsonMapper;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

public class UserListFlatMappingTopologyTest extends AbstractTopologyUnitTest {

    public static final String USER_LISTS_INPUT_TOPIC = "user-lists";
    public static final String USER_TO_GROUP_MAPPING_OUTPUT_TOPIC = "user-to-group-mapping";
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;

    private final UserMapper userMapper = new UserMapper(new JsonMapper());

    @Override
    protected Topology createTopology(StreamsBuilder streamsBuilder) {
        UserListFlatMappingTopology userListFlatMappingTopology = new UserListFlatMappingTopology(new StatefulFlatMapStreamFactory(streamsBuilder), userMapper);
        KStream<String, String> userLists = topicToStream(USER_LISTS_INPUT_TOPIC);
        KStream<String, String> userToGroupMappingStream = userListFlatMappingTopology.build(userLists);
        streamToTopic(userToGroupMappingStream, USER_TO_GROUP_MAPPING_OUTPUT_TOPIC);
        return streamsBuilder.build();
    }

    @Override
    protected void createTestTopics() {
        inputTopic = testInputTopic(USER_LISTS_INPUT_TOPIC);
        outputTopic = testOutputTopic(USER_TO_GROUP_MAPPING_OUTPUT_TOPIC);
    }

    @Test
    public void createsRecordForEachUserWithUserIdAsKeyAndGroupAsValue() {
        inputTopic.pipeInput("grp1", "[\"user1\", \"user2\", \"user3\"]");

        var records = outputTopic.readKeyValuesToList();
        assertThat(records).containsExactlyInAnyOrder(
                KeyValue.pair("user1", "grp1"),
                KeyValue.pair("user2", "grp1"),
                KeyValue.pair("user3", "grp1")
        );
    }

    @Test
    public void createsTombstone_whenUserIsRemovedFromList() {
        inputTopic.pipeInput("grp1", "[\"user1\", \"user2\"]");
        inputTopic.pipeInput("grp1", "[\"user1\"]");

        Map<String, String> records = outputTopic.readKeyValuesToMap();
        assertThat(records).containsExactly(
                entry("user1", "grp1"),
                entry("user2", null)
        );
    }
}
