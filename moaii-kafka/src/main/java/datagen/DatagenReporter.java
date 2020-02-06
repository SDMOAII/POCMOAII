package datagen;

import model.MoaiiEvent;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DatagenReporter implements Runnable
{
    private static final int NUM_MESSAGES = 10000;
    private final String TOPIC;
    private final Random random;
    public List<Integer> generatedIds;

    private Producer<String, MoaiiEvent> producer;

    public DatagenReporter(final Producer<String, MoaiiEvent> producer, String TOPIC)
    {
        this.producer = producer;
        this.TOPIC = TOPIC;
        this.generatedIds = new ArrayList<>();
        this.random = new Random();
    }

    @Override
    public void run()
    {
        Logger logger = LoggerFactory.getLogger(DatagenReporter.class);
        for (int i = 0; true; i++)
        {
            long time = System.currentTimeMillis();
           // System.out.println("Test Data #" + i + "from thread #" + Thread.currentThread().getId());

            MoaiiEvent event = createRecord();

            generatedIds.add(event.getIncidenceId());

            ProducerRecord<String, MoaiiEvent> record = null;

            if(random.nextInt(5) == random.nextInt(5))
            {
                //Sending records with repeated ids
                Integer repeatedId = generatedIds.get(random.nextInt(generatedIds.size()));
                record = new ProducerRecord<>(TOPIC, repeatedId.toString(), event);
            }
            else
            {
                record = new ProducerRecord<>(TOPIC, event.getEventId().toString(), event);
            }

            ProducerRecord<String, MoaiiEvent> finalRecord = record;
            producer.send(record, (recordMetadata, e) ->
            {
                if(e != null)
                {
                    System.out.println(e);
                    System.exit(1);
                }else
                {
                    System.out.println("Sended record : Key = " + finalRecord.key() + " value = " + finalRecord.value().toString());
                    System.out.println("Received new metadata. \n"+
                            "Topic: " + recordMetadata.topic() + "\n"+
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                    System.out.println("==========================================");
                }
            });
        }
        //System.out.println("Finished sending " + NUM_MESSAGES + " messages from thread #" + Thread.currentThread().getId() + "!");
    }

    private MoaiiEvent createRecord()
    {
        MoaiiEvent eventRecord = new MoaiiEvent();
        eventRecord.setIncidenceId(Math.abs(random.nextInt(10000)));
        eventRecord.setEventId(Math.abs(random.nextInt(100)));
        eventRecord.setTimestamp(LocalTime.now());

        return eventRecord;
    }
}

