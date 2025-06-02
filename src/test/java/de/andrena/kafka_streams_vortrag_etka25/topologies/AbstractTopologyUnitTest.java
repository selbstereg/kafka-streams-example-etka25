package de.andrena.kafka_streams_vortrag_etka25.topologies;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.Map;
import java.util.Properties;

public abstract class AbstractTopologyUnitTest {
    private StreamsBuilder streamsBuilder;
    private TopologyTestDriver testDriver;

    @BeforeEach
    public void setUp() {
        streamsBuilder = new StreamsBuilder();

        Topology topology = createTopology(streamsBuilder);

        var streamsConfig = createStreamsConfig();
        testDriver = new TopologyTestDriver(topology, streamsConfig);

        createTestTopics();
    }

    protected abstract Topology createTopology(StreamsBuilder streamsBuilder);

    protected abstract void createTestTopics();

    private static Properties createStreamsConfig() {
        return new KafkaStreamsConfiguration(Map.of(
                StreamsConfig.APPLICATION_ID_CONFIG, "test",
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "some value",
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass(),
                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass()
        )).asProperties();
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    protected TestInputTopic<String, String> testInputTopic(String topicName) {
        return testDriver.createInputTopic(topicName, Serdes.String().serializer(), Serdes.String().serializer());
    }

    protected TestOutputTopic<String, String> testOutputTopic(String topicName) {
        return testDriver.createOutputTopic(topicName, Serdes.String().deserializer(), Serdes.String().deserializer());
    }

    protected KStream<String, String> topicToStream(String topicName) {
        return streamsBuilder.stream(topicName, Consumed.with(Serdes.String(), Serdes.String()));
    }

    protected KTable<String, String> topicToTable(String topicName) {
        return streamsBuilder.table(topicName, Consumed.with(Serdes.String(), Serdes.String()));
    }

    protected void tableToTopic(KTable<String, String> table, String topicName) {
        streamToTopic(table.toStream(), topicName);
    }

    protected void streamToTopic(KStream<String, String> stream, String topicName) {
        stream.to(topicName, Produced.with(Serdes.String(), Serdes.String()));
    }
}
