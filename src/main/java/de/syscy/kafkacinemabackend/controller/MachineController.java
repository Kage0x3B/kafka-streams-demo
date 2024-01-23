package de.syscy.kafkacinemabackend.controller;

import de.syscy.kafkacinemabackend.data.MachineInfo;
import de.syscy.kafkacinemabackend.producer.KafkaProducer;
import lombok.AllArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@AllArgsConstructor
public class MachineController {
    private final StreamsBuilderFactoryBean factoryBean;
    private final KafkaProducer kafkaProducer;

    @PostMapping("/sendMachineInfo")
    public void sendMachineInfo(@RequestBody MachineInfo machineInfo) {
        kafkaProducer.sendMachineInfo(machineInfo);
    }

    @GetMapping("/getRecentProblems")
    public Map<String, String> getWordCount() {
        KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
        ReadOnlyKeyValueStore<String, String> machineProblemMessagesStore = kafkaStreams.store(StoreQueryParameters.fromNameAndType("machine-problem-messages", QueryableStoreTypes.keyValueStore()));

        Map<String, String> messages = new HashMap<>();
        try (KeyValueIterator<String, String> it = machineProblemMessagesStore.all()) {
            while (it.hasNext()) {
                KeyValue<String, String> entry = it.next();
                messages.put(entry.key, entry.value);
            }
        }

        return messages;
    }
}
