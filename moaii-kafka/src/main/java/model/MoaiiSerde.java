package model;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class MoaiiSerde implements Serde<MoaiiEvent>
{
    @Override
    public Serializer<MoaiiEvent> serializer()
    {
        return new CustomSerializer();
    }

    @Override
    public Deserializer<MoaiiEvent> deserializer()
    {
        return new CustomDeserializer();
    }
}


