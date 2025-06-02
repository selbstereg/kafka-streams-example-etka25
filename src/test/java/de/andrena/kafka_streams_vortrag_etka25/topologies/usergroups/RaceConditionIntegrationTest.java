package de.andrena.kafka_streams_vortrag_etka25.topologies.usergroups;

import de.andrena.kafka_streams_vortrag_etka25.TestConsumer;
import de.andrena.kafka_streams_vortrag_etka25.model.User;
import de.andrena.kafka_streams_vortrag_etka25.model.UserMapper;
import de.andrena.kafka_streams_vortrag_etka25.util.JsonMapper;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.*;
import java.util.stream.IntStream;

import static de.andrena.kafka_streams_vortrag_etka25.topologies.usergroups.UserGroupRootTopology.*;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Testcontainers
@TestPropertySource(properties = "spring.kafka.streams.properties.statestore.cache.max.bytes=0") // avoid internal compaction for easier debugging
class RaceConditionIntegrationTest {

    @Container
    static final ConfluentKafkaContainer kafka =
            new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
            .withCreateContainerCmdModifier((cmd) -> cmd.withName("kafka-testcontainer-" + new Random().nextInt(9999)));

    private static TestConsumer<List<User>> consumer;

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.kafka.streams.bootstrap-servers", kafka::getBootstrapServers);
    }

    @BeforeAll
    public static void setUp() {
        KafkaAdmin adminClient = new KafkaAdmin(new HashMap<>() {{
            put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        }});

        adminClient.createOrModifyTopics(TopicBuilder.name(USER_LISTS_INPUT_TOPIC)
                .partitions(1)
                .build());
        adminClient.createOrModifyTopics(TopicBuilder.name(USERS_INPUT_TOPIC)
                .partitions(1)
                .build());

        consumer = new TestConsumer<>(
                kafka.getBootstrapServers(),
                new UserMapper(new JsonMapper())::deserializeGroup
        );
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private UserMapper userMapper;

    @Test
    public void testRaceCondition() throws InterruptedException {
        int expectedGroupCount = 1000;

        // Create users and groups.
        // Each group contains one user
        // grp1 - [ "user1" ]
        // grp2 - [ "user2" ]
        // grp3...
        for (int i = 1; i <= expectedGroupCount; i++) {
            String groupId = "grp" + i;
            User user = new User("user" + i, "john doe", null);
            List<String> userList = List.of(user.id());
            kafkaTemplate.send(USER_LISTS_INPUT_TOPIC, groupId, userMapper.serialize(userList));
            kafkaTemplate.send(USERS_INPUT_TOPIC, user.id(), userMapper.serialize(user));
        }

        // Switch groups: each user goes to the next group
        // grp1 - [ "user1" ]     ==>    grp1 - [ ]
        // grp2 - [ "user2" ]     ==>    grp2 - [ "user1" ]
        // grp3 - [ "user3" ]     ==>    grp3 - [ "user2" ]
        // grp4 ...
        // first remove from old group
        for (int i = 1; i <= expectedGroupCount; i++) {
            String groupId = "grp" + i;
            kafkaTemplate.send(USER_LISTS_INPUT_TOPIC, groupId, "[]");
        }
        // then add to new group
        for (int i = 1; i <= expectedGroupCount; i++) {
            String newGroupId = "grp" + (i + 1);
            List<String> userList = List.of("user" + i);
            kafkaTemplate.send(USER_LISTS_INPUT_TOPIC, newGroupId, userMapper.serialize(userList));
        }

        Map<String, List<User>> userGroupMap = consumer.start(USER_GROUPS_OUTPUT_TOPIC);

//        Thread.sleep(10 * 1000);
//        for (int i = 1; i <= expectedGroupCount; i++) {
//            if (!userGroupMap.containsKey("grp" + (i + 1))) System.out.println("Missing group: grp" + (i+1) + " (should contain user" + i + ")");
//        }
//        Thread.sleep(15 * 60 * 1000);

        await().atMost(10, SECONDS)
                .pollInterval(1, SECONDS)
                .until(() -> expectedGroupCount == userGroupMap.size());

        await().atMost(10, SECONDS)
                .pollInterval(1, SECONDS)
                .until(() -> IntStream.range(1, expectedGroupCount+1).allMatch(i -> userGroupMap.containsKey("grp" + (i+1))));

        await().atMost(10, SECONDS)
                .pollInterval(1, SECONDS)
                .until(() -> IntStream.range(1, expectedGroupCount+1).allMatch(i ->
                        userGroupMap.get("grp" + (i+1)).get(0).id().equals("user" + i))
                );
    }

    @AfterAll
    public static void tearDown() {
        consumer.stop();
    }
}
