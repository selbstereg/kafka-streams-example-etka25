package de.andrena.kafka_streams_vortrag_etka25.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StatefulFlatMapProcessor implements Processor<String, String, String, String> {

    private final BiFunction<String, String, Stream<KeyValue<String, String>>> flatMapFunction;
    private final String stateStoreName;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private ProcessorContext<String, String> processorContext;
    private KeyValueStore<String, String> inputKeyToOutputKeysMapping;

    public StatefulFlatMapProcessor(BiFunction<String, String, Stream<KeyValue<String, String>>> flatMapFunction, String stateStoreName) {
        this.flatMapFunction = flatMapFunction;
        this.stateStoreName = stateStoreName;
    }

    @Override
    public void init(ProcessorContext<String, String> context) {
        inputKeyToOutputKeysMapping = context.getStateStore(stateStoreName);
        this.processorContext = context;
    }

    @Override
    public void process(Record<String, String> inputRecord) {
        if (inputRecord.value() == null) {
            List<Record<String, String>> outputTombstones = handleTombstone(inputRecord);
            outputTombstones.forEach(processorContext::forward);
        } else {
            List<Record<String, String>> outputRecords = handleNewInputRecord(inputRecord);
            outputRecords.forEach(processorContext::forward);
        }
    }

    private List<Record<String, String>> handleNewInputRecord(Record<String, String> inputRecord) {
        String inputKey = inputRecord.key();
        String inputValue = inputRecord.value();

        List<String> oldKeys = getOldKeys(inputKey);
        List<Record<String, String>> outputRecords = flatMapFunction.apply(inputKey, inputValue)
                .map(kv -> new Record<>(kv.key, kv.value, inputRecord.timestamp()))
                .collect(Collectors.toList());
        List<String> keysToEmit = outputRecords.stream().map(Record::key).collect(Collectors.toList());

        // TODO: Use JsonMapper which already contains the RuntimeException conversion
        try {
            inputKeyToOutputKeysMapping.put(inputKey, objectMapper.writeValueAsString(keysToEmit));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error serializing output keys", e);
        }

        Set<String> keysToDelete = new HashSet<>(oldKeys);
        keysToDelete.removeAll(keysToEmit);

        List<Record<String, String>> tombstones = createTombstonesForRemovedKeys(keysToDelete, inputRecord.timestamp());
        outputRecords.addAll(tombstones);

        return outputRecords;
    }

    private List<String> getOldKeys(String inputKey) {
        String oldKeysJson = inputKeyToOutputKeysMapping.get(inputKey);
        if (oldKeysJson != null) {
            try {
                return objectMapper.readValue(oldKeysJson, new TypeReference<>() {
                });
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Error deserializing old keys", e);
            }
        }
        return Collections.emptyList();
    }

    private List<Record<String, String>> createTombstonesForRemovedKeys(Set<String> removedKeys, long timestamp) {
        return removedKeys.stream()
                .map(outputKey -> new Record<>(outputKey, (String) null, timestamp))
                .collect(Collectors.toList());
    }

    private List<Record<String, String>> handleTombstone(Record<String, String> inputRecord) {
        String oldKeysJson = inputKeyToOutputKeysMapping.get(inputRecord.key());
        if (oldKeysJson == null) {
            return Collections.emptyList();
        }

        inputKeyToOutputKeysMapping.delete(inputRecord.key());

        try {
            List<String> oldKeys = objectMapper.readValue(oldKeysJson, new TypeReference<>() {
            });
            return oldKeys.stream() // TODO: Duplicate code. See createTombstonesForRemovedKeys(Set<String> removedKeys)
                    .map(outputKey -> new Record<>(outputKey, (String) null, inputRecord.timestamp()))
                    .collect(Collectors.toList());
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error deserializing old keys for tombstone", e);
        }
    }
}
