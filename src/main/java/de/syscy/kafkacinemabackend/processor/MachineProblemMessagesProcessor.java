package de.syscy.kafkacinemabackend.processor;

import de.syscy.kafkacinemabackend.data.MachineInfo;
import de.syscy.kafkacinemabackend.data.MachineStatus;
import de.syscy.kafkacinemabackend.data.serialization.CustomSerializers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MachineProblemMessagesProcessor {
    private static final Serde<String> STRING_SERDE = Serdes.String();

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, MachineInfo> messageStream = streamsBuilder.stream("machine-status", Consumed.with(STRING_SERDE, CustomSerializers.MACHINE_INFO_SERDE));

        messageStream
                .filter((key, value) -> value.status() == MachineStatus.ERROR)
                .mapValues(MachineInfo::statusMessage).to("machine-problem-messages");

        /*KTable<String, Long> wordCounts = messageStream.mapValues(value -> value.toLowerCase())
                .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
                .groupBy((key, word) -> word, Grouped.with(STRING_SERDE, STRING_SERDE))
                .count(Materialized.as("counts"));

        wordCounts.toStream().to("output-topic");*/
    }
}
