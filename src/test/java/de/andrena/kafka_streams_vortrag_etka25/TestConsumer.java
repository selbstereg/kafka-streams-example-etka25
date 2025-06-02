package de.andrena.kafka_streams_vortrag_etka25;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class TestConsumer<V> {
    private final Properties props = new Properties();
    private final Function<String, V> valueMapper;
    private volatile boolean stopped = false;
    private SynchronizedConsumer consumer;


    public TestConsumer(String bootstrapServers, Function<String, V> valueMapper) {
        this.valueMapper = valueMapper;
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    public Map<String, V> start(String topic) {
        Map<String, V> recordsMap = new ConcurrentHashMap<>();
        new Thread(
                () -> {
                    consumer = new SynchronizedConsumer(props);
                    consumer.subscribe(topic);
                    while (!stopped) {
                        ConsumerRecords<String, String> polledRecords = consumer.poll();
                        polledRecords.forEach(record -> {
                            if (record.value() == null) {
                                recordsMap.remove(record.key());
                            } else {
                                V value = valueMapper.apply(record.value());
                                recordsMap.put(record.key(), value);
                            }
                        });
                    }
                }
        ).start();
        return recordsMap;
    }

    public AtomicInteger countMessages(String topic) {
        AtomicInteger atomicInteger = new AtomicInteger(0);
        new Thread(
                () -> {
                    consumer = new SynchronizedConsumer(props);
                    consumer.subscribe(topic);
                    while (!stopped) {
                        ConsumerRecords<String, String> polledRecords = consumer.poll();
                        polledRecords.forEach(record -> {
                            atomicInteger.incrementAndGet();
                        });
                    }
                }
        ).start();
        return atomicInteger;
    }

    public void stop() {
        stopped = true;
        consumer.close();
    }

    private static class SynchronizedConsumer {
        private final KafkaConsumer<String, String> consumer;
        SynchronizedConsumer(Properties props) {
            consumer = new KafkaConsumer<>(props);
        }

        synchronized void subscribe(String topic) {
            consumer.subscribe(Collections.singletonList(topic));
        }

        synchronized ConsumerRecords<String, String> poll() {
            return consumer.poll(Duration.ofMillis(100));
        }

        synchronized void close() {
            consumer.close();
        }
    }
}
