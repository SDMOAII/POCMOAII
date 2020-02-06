package datagen;


import model.CustomSerializer;
import model.MoaiiEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DatagenProducer
{
    private final static String TOPIC = "moaii_random";
    private final static int NUM_THREADS = 2;
    private final static String KAFKA_SERVER = "localhost:9092";

    public static void main(String... args) throws Exception
    {
        //create kafka producer
        final Producer<String, MoaiiEvent> producer = createProducer();
        //Create an executor for threading
        final ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

        for(int i = 0; i < NUM_THREADS; i ++)
        {
            executor.execute(new DatagenReporter(producer, TOPIC));
        }

    }

    private static Producer<String, MoaiiEvent> createProducer()
    {
        try
        {
            Properties properties = new Properties();
            properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomSerializer.class.getName());

            return new KafkaProducer<String, MoaiiEvent>(properties);
        }
        catch (Exception e)
        {
            System.out.println("Failed to create producer with exception: " + e);
            System.exit(0);
            return null;
        }
    }
}
