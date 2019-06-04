import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;



public class ConsumerLifecycleManager implements LifecycleManager {

    private static Logger logger = LoggerFactory.getLogger(ConsumerLifecycleManager.class.getName());
    static KafkaConsumer<String,String> consumer = null;


    public static KafkaConsumer<String,String> getConsumerInstance(){
        // Criar as propriedades do consumidor
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer .class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer_demo");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Criar o consumidor
        KafkaConsumer<String ,String> consumer = new KafkaConsumer<String, String>(properties);
        return consumer;
    }

    public void start(){
        if (consumer == null) {
            consumer = getConsumerInstance();
            // Subscrever o consumidor para o nosso(s) tópico(s)
            consumer.subscribe(Collections.singleton("meu_topico"));
        }
        while (consumer != null) {  // Apenas como demo, usaremos um loop infinito
            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord record : poll) {
                logger.info(record.topic() + " - " + record.partition() + " - " + record.value());
            }
        }
    }

    public void stop(){
        // Close Producer
        consumer.close();
        consumer = null;
    }

}


