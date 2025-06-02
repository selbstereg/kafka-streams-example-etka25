package de.andrena.kafka_streams_vortrag_etka25.util;

/*import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class TopologyConfigurationUtil {
    private final SpecificAvroSerde<?> specificAvroSerdeForValue;

    public <V extends SpecificRecord> Materialized<String, V, KeyValueStore<Bytes, byte[]>> materialized(String storeName) {
        return Materialized
                .<String, V, KeyValueStore<Bytes, byte[]>>as(storeName)
                .withKeySerde(Serdes.String())
                .withValueSerde(castSerde(specificAvroSerdeForValue));
    }

    public  <V extends SpecificRecord> Grouped<String, V> groupedSpecific(String name) {
        return Grouped
                .<String, V>as(name)
                .withKeySerde(Serdes.String())
                .withValueSerde(castSerde(specificAvroSerdeForValue));
    }

    @SuppressWarnings("unchecked")
    public static <T> Serde<T> castSerde(Serde<?> serde) {
        return (Serde<T>) serde;
    }

    @SuppressWarnings("unchecked")
    public static <T> PrimitiveAvroSerde<T> castSerde(PrimitiveAvroSerde<?> serde) {
        return (PrimitiveAvroSerde<T>) serde;
    }

    @SuppressWarnings("unchecked")
    public static <T extends SpecificRecord> SpecificAvroSerde<T> castSerde(SpecificAvroSerde<?> serde) {
        return (SpecificAvroSerde<T>) serde;
    }
}*/
