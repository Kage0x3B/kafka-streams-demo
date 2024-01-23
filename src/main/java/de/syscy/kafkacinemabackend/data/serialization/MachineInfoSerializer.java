package de.syscy.kafkacinemabackend.data.serialization;

import de.syscy.kafkacinemabackend.data.MachineInfo;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class MachineInfoSerializer implements Serializer<MachineInfo> {
    @Override
    public byte[] serialize(String topic, MachineInfo data) {
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
             ObjectOutputStream objectStream = new ObjectOutputStream(byteStream)) {
            objectStream.writeObject(data);

            return byteStream.toByteArray();
        } catch (IOException ex) {
            throw new RuntimeException("Serialization of MachineInfo failed", ex);
        }
    }
}
