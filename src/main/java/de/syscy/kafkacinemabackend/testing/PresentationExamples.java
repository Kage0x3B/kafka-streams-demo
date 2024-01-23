package de.syscy.kafkacinemabackend.testing;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Objects;

public class PresentationExamples {
    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();

        // Filter

        builder.stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
                .filter((key, value) -> Objects.equals(value, "error"))
                .to("output-topic");

        // Map

        builder.stream("input-topic", Consumed.with(Serdes.String(), Serdes.Integer()))
                .mapValues((value) -> value * 2)
                .to("output-topic");

        // Aggregate

        builder.stream("input-topic", Consumed.with(Serdes.String(), Serdes.Integer()))
                .groupByKey()
                .aggregate(() -> 0, (key, value, accumulator) -> accumulator + value)
                .toStream()
                .to("output-topic");


        // Join Example

        KStream<String, Double> machineTemperature = builder.stream("machine-contents-temperature", Consumed.with(Serdes.String(), Serdes.Double()));
        KStream<String, Integer> machineFillLevel = builder.stream("machine-fill-level", Consumed.with(Serdes.String(), Serdes.Integer()));

        machineTemperature.join(machineFillLevel, (temperature, fillLevel) ->
                        String.format("[Ice Cream Machine Status] Temperature: %.1f °C, Fill Level: %d cm", temperature, fillLevel), JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1)))
                .to("machine-status", Produced.with(Serdes.String(), Serdes.String()));


        double temperature = -4.3;
        int fillLevel = 7;

        // Second Join Example - User Points + Username

        KStream<String, Integer> userPointsStream = builder.stream("user-points", Consumed.with(Serdes.String(), Serdes.Integer()));
        KTable<String, String> usernameTable = builder.table("user-username", Consumed.with(Serdes.String(), Serdes.String()));

        userPointsStream.leftJoin(usernameTable, (userPoints, username) -> {
            if (userPoints != null) {
                return String.format("%s has %d points in total!", username, userPoints);
            } else {
                return String.format("%s has no points.", username);
            }
        }).to("user-point-announcements", Produced.with(Serdes.String(), Serdes.String()));

        builder.stream("input-topic", Consumed.with(Serdes.String(), Serdes.Integer()))
                .groupByKey()
                .aggregate(() -> 0, (key, value, accumulator) -> accumulator + value)
                .toStream()
                .to("output-topic");
    }
}
