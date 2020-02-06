import model.MoaiiEvent;
import model.MoaiiSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Suppressed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class StreamsApp
{
    public static void main(String[] args)
    {
        Logger logger = LoggerFactory.getLogger(StreamsApp.class);

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "moaii-streams");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MoaiiSerde.class);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, MoaiiEvent> input =  builder.stream("moaii_random");


        input
        .peek((key, value) -> System.out.println("Ktable : key=" + key + ", value="+ value ))
        .groupByKey()
        .windowedBy(SessionWindows.with(Duration.ofSeconds(10)))
        .count(Materialized.with(Serdes.String(), Serdes.Long()))
        .suppress(Suppressed.untilTimeLimit(Duration.ofSeconds(10), unbounded()))
        .filter((stringWindowed, count) -> count == 1)
        .toStream()
        .to("moaii_random_filtered");



        final Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, config);

        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        //print the topology every 10 seconds for development purposes
        while(true)
        {
            System.out.println(topology.describe());
            try
            {
                Thread.sleep(10000);
            } catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
    }
}
