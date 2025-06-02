package de.andrena.kafka_streams_vortrag_etka25.util;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class TopicConnector {
    private final StreamsBuilder streamsBuilder;

    public KStream<String, String> readInputTopic(String topicName) {
        return streamsBuilder.stream(topicName);
    }

    public void writeToTopic(KTable<String, String> table, String topicName) {
        writeToTopic(table.toStream(), topicName);
    }

    public void writeToTopic(KStream<String, String> stream, String topicName) {
        stream.to(topicName);
    }
}
