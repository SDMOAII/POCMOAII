package com.msa;

import avro.MoaiiEvent;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class MoaiiProducer
{
    private final static String KAFKA_SERVER = "localhost:9092";
    private final static String TOPIC = "moaii-incidences";
    private final static String SCHEMA_REGISTRY_URL = "localhost:8081";
    private static List<Integer> generatedIds = new ArrayList<>();

    public static void main(String[] args)
    {
        Properties config = new Properties();

        //Kafka bootstrap server
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        //Producer acks
        config.put(ProducerConfig.ACKS_CONFIG, "all"); //strongest producing guarantee
        config.put(ProducerConfig.RETRIES_CONFIG, "3");
        config.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        //leverage idempotent producer
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        config.put("schema.registry.url", SCHEMA_REGISTRY_URL);

        Producer<String, MoaiiEvent> producer = new KafkaProducer(config);

        int i = 0;
        while (true)
        {
            System.out.println("Producing record: " + i);
            try
            {
                producer.send(createRecord());
                //producer.send(createRecord2());
                Thread.sleep(100);

            } catch (InterruptedException e)
            {
                break;
            }
            i++;
        }
        producer.close();
    }

    private static ProducerRecord<String, MoaiiEvent> createRecord()
    {
        MoaiiEvent.Builder eventBuilder = MoaiiEvent.newBuilder();
        //random incidenceId / key
        Integer incidenceId = ThreadLocalRandom.current().nextInt(0, 9999);
        eventBuilder.setIncidenceId(incidenceId);
        Integer eventId = ThreadLocalRandom.current().nextInt(0, 99);
        eventBuilder.setEventId(eventId);
        Long timeMillis = System.currentTimeMillis();
        eventBuilder.setTime(timeMillis);

        return new ProducerRecord<>(TOPIC, incidenceId.toString(), eventBuilder.build());
    }

    private static ProducerRecord<String, String> createRecord2()
    {
        //Empty json
        ObjectNode incidence = JsonNodeFactory.instance.objectNode();
        //random incidenceId / key
        Integer incidenceId = ThreadLocalRandom.current().nextInt(0, 9999);
        generatedIds.add(incidenceId);
        //random eventId
        Integer eventId = ThreadLocalRandom.current().nextInt(0, 99);
        Long timeMillis = System.currentTimeMillis();
        //Write new moaii event into a json doc
        incidence.put("incidenceId", incidenceId);
        incidence.put("eventId", eventId);
        incidence.put("time", timeMillis);
        //repeating ids 1/25 of the times
        if (ThreadLocalRandom.current().nextInt(5) == ThreadLocalRandom.current().nextInt(5))
        {
            String repeatedId = generatedIds
                    .get(ThreadLocalRandom.current()
                            .nextInt(0, generatedIds.size())).toString();

            return new ProducerRecord<>(TOPIC, repeatedId, incidence.toString());
        }
        return new ProducerRecord<>(TOPIC, incidenceId.toString(), incidence.toString());
    }

}
