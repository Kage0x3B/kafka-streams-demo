package de.syscy.kafkacinemabackend.data.serialization;

import de.syscy.kafkacinemabackend.data.MachineInfo;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.*;

public class MachineInfoDeserializer implements Deserializer<MachineInfo> {
    @Override
    public MachineInfo deserialize(String s, byte[] data) {
        try (ByteArrayInputStream byteStream = new ByteArrayInputStream(data);
             ObjectInputStream objectStream = new ObjectInputStream(byteStream)) {
            return (MachineInfo) objectStream.readObject();
        } catch (IOException | ClassNotFoundException ex) {
            throw new RuntimeException("Deserialization of MachineInfo failed", ex);
        }
    }
}
