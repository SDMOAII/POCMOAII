package model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class CustomSerializer implements Serializer<MoaiiEvent>
{

    @Override
    public void configure(Map<String, ?> configs, boolean isKey)
    {
    }

    @Override
    public byte[] serialize(String s, MoaiiEvent o)
    {
        byte[] result = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try
        {
            result = objectMapper.writeValueAsBytes(o);
        }
        catch (JsonProcessingException e)
        {
            e.printStackTrace();
        }
        return result;
    }


    @Override
    public void close()
    {
    }
}

