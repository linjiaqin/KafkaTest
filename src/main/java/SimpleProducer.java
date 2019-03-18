import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class SimpleProducer {
    public static void main(String[] args) {
        //String topicname = "multest";
        String topicname = "flumeTest";
        //为了存放相当于命令行中的那些参数
        Properties props = new Properties();
        //Assign localhost id
        //和broke-list有什么区别?

        //props.put("bootstrap.servers", "localhost:9094");
        props.put("bootstrap.servers", "localhost:9092");
        //Set acknowledgements for producer requests.
        props.put("acks", "all");

                //If the request fails, the producer can automatically retry,
                props.put("retries", 0);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);


        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");


        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        String senddata[] = {"my","name", "is","lin","jia","qin","ha","ha","lin"};
        for (int i = 0; i < senddata.length; i++) {
            producer.send(new ProducerRecord<String, String>(topicname,senddata[i],Integer.toString(i)));
        }
        System.out.println("finish");
        producer.close();
    }
}
