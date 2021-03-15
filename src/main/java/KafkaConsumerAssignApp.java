import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Properties;

public class KafkaConsumerAssignApp {
    public static void Main(String  [] args)
    {
        Properties prop=new Properties();
        prop.put("bootstrap.servers","localhost:9092, localhost:9093");
        prop.put("key.deseriazlizer","org.apache.kafka.common.serializer.StringDeserializer");
        prop.put("value.deseriazlizer","org.apache.kafka.common.serializer.StringDeserializer");
        prop.put("group.id","test");

        KafkaConsumer myConsumer = new KafkaConsumer(prop);

        ArrayList<TopicPartition> partitions=new ArrayList<TopicPartition>();
        TopicPartition myTopicPart0=new TopicPartition("my-topic",0);
        TopicPartition myTopicPart2=new TopicPartition("my-other-topic",2);
        partitions.add(myTopicPart0);
        partitions.add(myTopicPart2);

        myConsumer.assign(partitions);
        try
        {
            while(true)
            {
                ConsumerRecords<String,String> records=myConsumer.poll(10);
                for(ConsumerRecord<String,String> record:records)
                {
                    System.out.println(
                            String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s",
                                    record.topic(),record.partition(),record.offset(),record.key(),record.value())
                    );
                }
            }

        }
        catch (Exception e)
        {
            System.out.println(e.getStackTrace());
        }
        finally {
            myConsumer.close();
        }
    }
}
