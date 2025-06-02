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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static de.andrena.kafka_streams_vortrag_etka25.topologies.usergroups.UserGroupRootTopology.*;
import static org.apache.commons.collections4.CollectionUtils.union;

@SpringBootTest
@Testcontainers
//@TestPropertySource(properties = "spring.kafka.streams.properties.statestore.cache.max.bytes=0")
public class DataExplosionIllustration {

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

        consumer = new TestConsumer<>(kafka.getBootstrapServers(), new UserMapper(new JsonMapper())::deserializeGroup);
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private UserMapper userMapper;

    @Test
    public void illustrateDataExplosion() throws InterruptedException {
        List<String> group1 = List.of("user1", "user2", "user3", "user4", "user5", "user6", "user7", "user8",
                "user9", "user10");
        List<String> group2 = List.of("user11", "user12", "user13", "user14", "user15", "user16", "user17", "user18",
                "user19", "user20");

        // Send the initial user lists
        kafkaTemplate.send(USER_LISTS_INPUT_TOPIC, "group1", userMapper.serialize(group1));
        kafkaTemplate.send(USER_LISTS_INPUT_TOPIC, "group2", userMapper.serialize(group2));

        // create the Users
        union(group1, group2).forEach(userId ->
                kafkaTemplate.send(USERS_INPUT_TOPIC, userId, userMapper.serialize(new User(userId, "-", null))));

        // Move a user from group1 to group2
        List<String> group1Updated = new ArrayList(group1);
        List<String> group2Updated = new ArrayList(group2);
        String movedUser = group1Updated.remove(0);
        group2Updated.add(movedUser);

        // Update the user lists
        kafkaTemplate.send(USER_LISTS_INPUT_TOPIC, "group1", userMapper.serialize(group1Updated));
        kafkaTemplate.send(USER_LISTS_INPUT_TOPIC, "group2", userMapper.serialize(group2Updated));

        AtomicInteger outputMessageCount = consumer.countMessages(USER_GROUPS_OUTPUT_TOPIC);

        Thread.sleep(10 * 1000); // Wait for processing

        System.out.println("Number of consumed messages: " + outputMessageCount);
    }

    @AfterAll
    public static void tearDown() {
        consumer.stop();
    }
}
