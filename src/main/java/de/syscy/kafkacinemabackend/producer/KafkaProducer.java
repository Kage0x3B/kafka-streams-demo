package de.syscy.kafkacinemabackend.producer;

import de.syscy.kafkacinemabackend.data.MachineInfo;
import de.syscy.kafkacinemabackend.data.serialization.MachineInfoSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Configuration
@Slf4j
@Component
public class KafkaProducer {

    @Value(value = "${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

    public KafkaTemplate<String, MachineInfo> kafkaTemplate;

    public KafkaProducer(
            @Autowired
            KafkaTemplate<String, MachineInfo> machineInfoKafkaTemplate) {
        this.kafkaTemplate = machineInfoKafkaTemplate;
    }

    public Map<String, Object> machineInfoProducerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);

        return props;
    }

    public ProducerFactory<String, MachineInfo> machineInfoProducerFactory() {
        return new DefaultKafkaProducerFactory<>(machineInfoProducerConfigs(), new StringSerializer(), new MachineInfoSerializer());
    }

    public KafkaTemplate<String, MachineInfo> machineInfoKafkaTemplate() {
        return new KafkaTemplate<>(machineInfoProducerFactory());
    }


    public void sendMachineInfo(MachineInfo machineInfo) {
        kafkaTemplate.send("machine-status", machineInfo)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Message sent to topic machine-status: {}", machineInfo);
                    } else {
                        log.error("Failed to send message", ex);
                    }
                });
    }
}
