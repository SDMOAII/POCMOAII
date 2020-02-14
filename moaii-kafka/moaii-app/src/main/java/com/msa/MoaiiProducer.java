package com.msa;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class MoaiiProducer
{
    private final static String KAFKA_SERVER = "http://localhost:9092";
    private final static String EVENT_TOPIC = "events";
    private final static String INCIDENCE_TOPIC = "incidences";
    private final static String SCHEMA_REGISTRY_URL = "http://localhost:8081";
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

        Producer<Long, MoaiiEvent> eventProducer = new KafkaProducer(config);
        Producer<Long, Integer> incidenceProducer = new KafkaProducer(config);

        int i = 0;
        while (true)
        {
            try
            {
                ProducerRecord<Long, MoaiiEvent> eventRecord = createEventRecord();
//                ProducerRecord<Long, Integer> incidenceRecord = new ProducerRecord<Long, Integer>(INCIDENCE_TOPIC, eventRecord.key(), 50);

//                incidenceProducer.send(incidenceRecord,
//                        ((metadata, exception) ->
//                        {
//                            if (exception == null)
//                            {
//                                System.out.println("incidence sended");
//                                System.out.println(metadata.toString());
//                                System.out.println("*************************");
//                            }
//                            else
//                            {
//                                exception.printStackTrace();
//                            }
//                        }));

                eventProducer.send(eventRecord,
                        (metadata, exception) ->
                        {
                            if (exception == null)
                            {
                                System.out.println("event sended");
                                System.out.println(metadata.toString());
                                System.out.println("*************************");
                            } else
                            {
                                exception.printStackTrace();
                            }
                        });
                //producer.send(createRecord2());
                Thread.sleep(ThreadLocalRandom.current().nextInt(0, 10000));

            } catch (InterruptedException e)
            {
                break;
            }
            i++;
        }
        eventProducer.flush();
        eventProducer.close();
    }

    private static ProducerRecord<Long, MoaiiEvent> createEventRecord()
    {
        MoaiiEvent.Builder eventBuilder = MoaiiEvent.newBuilder();
        //random incidenceId / key
        Integer incidenceId = ThreadLocalRandom.current().nextInt(0, 9999);
        eventBuilder.setIncidenceId(incidenceId);
        generatedIds.add(incidenceId);

        Integer eventId = ThreadLocalRandom.current().nextInt(0, 99);
        eventBuilder.setEventId(eventId);
        Long timeMillis = System.currentTimeMillis();
        eventBuilder.setTime(timeMillis);

        //type 5 = cancel
        eventBuilder.setIncidenceType(ThreadLocalRandom.current().nextInt(0, 5));
        //repeating ids 1/25 of the times
        if (ThreadLocalRandom.current().nextInt(5) == ThreadLocalRandom.current().nextInt(5))
        {
            Integer repeatedId = generatedIds
                    .get(ThreadLocalRandom.current()
                            .nextInt(0, generatedIds.size()));
            eventBuilder.setIncidenceId(repeatedId);
        }

        return new ProducerRecord<>(EVENT_TOPIC, eventBuilder.build());
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

            return new ProducerRecord<>(EVENT_TOPIC, repeatedId, incidence.toString());
        }
        return new ProducerRecord<>(EVENT_TOPIC, incidenceId.toString(), incidence.toString());
    }

}
