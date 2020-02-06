package com.msa;

import avro.MoaiiEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Suppressed;

import java.time.Duration;
import java.util.Properties;

public class MoaiiAvroApp
{
    private final static String KAFKA_SERVER = "localhost:9092";

    public static void main(String[] args)
    {
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "moaii-application-demo");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); //We read from the start of the topic

        //disable the cache to demonstrate all the "steps" involved in the transformation
        // NOT RECOMMENDED IN PRODUCTION
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        //Exactly one processing
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);

        //json SerDe
//        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
//        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
//        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
//        final SessionWindowedSerde sessionKeySerde = new SessionWindowedSerde(Serdes.String());
//        final Produced produced = Produced.with(sessionKeySerde, Serdes.Long());
////        final Produced produced = Produced.with(sessionKeySerde, jsonSerde);
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, MoaiiEvent> events = builder.stream("moaii-incidences");

       // Produced<String, JsonNode> produced = Produced.with(Serdes.String(), jsonSerde);
        events
            .peek((key, value) -> System.out.println("Ktable : key=" + key + ", value="+ value ))
            .groupByKey()
            .windowedBy(SessionWindows.with(Duration.ofSeconds(10)))
            .count()
            .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
            .filter((key, value) -> value.equals(1))
            .toStream()
            .to("moaii-missing-events");


        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, config);
        streams.cleanUp();
        streams.start();

        //print the topology
        System.out.println(topology.describe());

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
