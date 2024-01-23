package de.syscy.kafkacinemabackend.data;

public record MachineInfo(String machineName, MachineStatus status, String statusMessage) {}
