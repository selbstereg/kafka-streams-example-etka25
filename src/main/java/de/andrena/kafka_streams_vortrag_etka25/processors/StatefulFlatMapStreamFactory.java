package de.andrena.kafka_streams_vortrag_etka25.processors;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.stereotype.Component;

import java.util.function.BiFunction;
import java.util.stream.Stream;

@Component
@RequiredArgsConstructor
public class StatefulFlatMapStreamFactory {

    private final StreamsBuilder streamsBuilder;

    public KStream<String, String> create(
            KStream<String, String> inputStream,
            BiFunction<String, String, Stream<KeyValue<String, String>>> flatMapFunction,
            String stateStoreName
    ) {
        StoreBuilder<KeyValueStore<String, String>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(stateStoreName),
                        org.apache.kafka.common.serialization.Serdes.String(),
                        org.apache.kafka.common.serialization.Serdes.String());
        streamsBuilder.addStateStore(storeBuilder);

        return inputStream.process(
                () -> new StatefulFlatMapProcessor(flatMapFunction, stateStoreName),
                Named.as("stateful-flat-map"),
                stateStoreName
        );
    }
}
