package de.syscy.kafkacinemabackend.data.serialization;

import de.syscy.kafkacinemabackend.data.MachineInfo;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class CustomSerializers {
    public static final Serde<MachineInfo> MACHINE_INFO_SERDE = Serdes.serdeFrom(new MachineInfoSerializer(), new MachineInfoDeserializer());
}
