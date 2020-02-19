package com.msa;


import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.SessionStore;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

public class MoaiiAvroApp
{
    private final static String KAFKA_SERVER = "http://localhost:9092";
    private final static String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    public static void main(String[] args)
    {
        MoaiiAvroApp app = new MoaiiAvroApp();
        app.start();
    }

    private void start()
    {
        Properties config = getKafkaConfig();
        KafkaStreams streams = createTopology(config);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private Properties getKafkaConfig()
    {
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "moaii-application-demo");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        ;
        config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);

        return config;
    }

    private KafkaStreams createTopology(Properties config)
    {
        SpecificAvroSerde<MoaiiEvent> moaiiEventSerde = new SpecificAvroSerde<>();
        moaiiEventSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL), false);

        SpecificAvroSerde<MoaiiEventState> moaiiStateSerde = new SpecificAvroSerde<MoaiiEventState>();
        moaiiStateSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL), false);



        StreamsBuilder builder = new StreamsBuilder();

        Duration windowSizeDuration = Duration.ofSeconds(10);
        KStream<Long, MoaiiEvent> eventStream = builder
                .stream("events", Consumed.with(Serdes.Long(), moaiiEventSerde))
                .selectKey((key, event) -> event.getIncidenceId());

      SessionWindows sessionWindow = SessionWindows.with(Duration.ofSeconds(5));

        //Since our topic has no key, we select the key(incidenceId) from Payload
        //Didn't write the event including the key just for Development test and learning
        SessionWindowedKStream<Long, MoaiiEvent> windowedEventTable = eventStream
               .peek(((key, event) -> System.out.println("Key : " + key + ", Event : " + event)))
                .groupByKey()
                .windowedBy(sessionWindow);

         //Detecting missing events
           KTable<Windowed<Long>, Long> missingEventTable = windowedEventTable
                .count(
                        Materialized.<Long, Long, SessionStore<Bytes, byte[]>>as("timeout-incidences")
                                .withValueSerde(Serdes.Long())
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));

         //detecting patterns
     KTable<Windowed<Long>, MoaiiEventState> patternTable = windowedEventTable
                .aggregate(
                        () -> new MoaiiEventState(0L,new HashMap<String,Integer>()),
                        (key, value, aggregate) ->
                        {
                          //  aggregate.put(Long.toString(value.getTime()), value.getIncidenceType());
                           aggregate.getState().put(Long.toString(value.getTime()),  value.getIncidenceType());
                           return aggregate;
                        },
                        (aggKey, aggOne, aggTwo) ->
                        {
                            MoaiiEventState finalState = new MoaiiEventState();
                            finalState.getState().putAll(aggOne.getState());
                            finalState.getState().putAll((aggTwo.getState()));
                            return finalState;
                        },
                        Materialized.as("pattern-state-store")

                );

        //write results to a topic
        missingEventTable
                .toStream()
                .peek(((key, event) -> System.out.println("Missing event  " +  "Key : " + key + ", Event : " + event)))
                .to("missing-events", Produced.with(WindowedSerdes.sessionWindowedSerdeFrom(Long.class), Serdes.Long()));

        patternTable
                .toStream()
                .peek(((key, event) -> System.out.println("Pattern event  " +  "Key : " + key + ", Event : " + event)))
                .to("patterns");/**/

        Topology topology = builder.build();
        System.out.println(topology.describe());
        return new KafkaStreams(topology, config);
    }

}  
