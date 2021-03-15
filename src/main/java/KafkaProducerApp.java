import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerApp {

    public static void main( String [] args)
    {

        Properties prop=new Properties();
        prop.put("bootstrap.servers","localhost:9092, localhost:9093");
        prop.put("key.seriazlizer","org.apache.kafka.common.serializer.StringSerializer");
        prop.put("value.seriazlizer","org.apache.kafka.common.serializer.StringSerializer");

        KafkaProducer<String, String> myProducer = new KafkaProducer<String,String>(prop);

        try
        {
            for (int i = 0; i < 150; i++) {
                myProducer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), "MyMessage: " + Integer.toString(i)));
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            myProducer.close();
        }

    }
}
