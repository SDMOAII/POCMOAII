package model;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class CustomDeserializer implements Deserializer<MoaiiEvent>
{
    @Override
    public void configure(Map configs, boolean isKey)
    {

    }

    @Override
    public MoaiiEvent deserialize(String s, byte[] bytes)
    {
        ObjectMapper mapper = new ObjectMapper();
        MoaiiEvent event = null;

        try
        {
            event = mapper.readValue(bytes, MoaiiEvent.class);
        }
        catch (JsonMappingException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return event;
    }

    @Override
    public MoaiiEvent deserialize(String topic, Headers headers, byte[] data)
    {
        return null;
    }

    @Override
    public void close()
    {

    }
}
