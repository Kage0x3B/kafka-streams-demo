package de.syscy.kafkacinemabackend.testing;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

public class ManualKafka {
    private static Map<String, Object> getProperties() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("bootstrap.servers", "localhost:29092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        return properties;
    }

    public static void main(String[] args) {
        try (Consumer<String, String> consumer = new KafkaConsumer<>(getProperties()); Producer<String, String> producer = new KafkaProducer<>(getProperties())) {
            consumer.subscribe(Collections.singletonList("machine-status-test"));

            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(3));

                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    String machineStatus = consumerRecord.value();

                    if (Objects.equals(machineStatus, "error")) {
                        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("machine-problems-test", consumerRecord.key(), machineStatus);
                        producer.send(producerRecord, ((recordMetadata, exception) -> {
                            // TODO: Error handling
                        }));
                    }
                }
            }

        }
    }
}
