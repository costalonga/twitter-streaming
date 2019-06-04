//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.common.serialization.StringDeserializer;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.time.Duration;
//import java.util.Collections;
//import java.util.Properties;
//
//public class ConsumerDemoWithDeserializer {
//
//    private static Logger logger = LoggerFactory.getLogger(ConsumerDemoWithDeserializer.class.getName());
//
//    public static void main(String[] args){
//
//        // Criar as propriedades do consumidor
//        Properties properties = new Properties();
//        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserDeserializer.class.getName());
//        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumer_demo");
//        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//
//        // Criar o consumidor
//        KafkaConsumer<String ,User> consumer = new KafkaConsumer<>(properties);
//
//        // Subscrever o consumidor para o nosso(s) tópico(s)
//        consumer.subscribe(Collections.singleton("meu_topico"));
//
//        // Ler as mensagens
//        while (true) {  // Apenas como demo, usaremos um loop infinito
//            ConsumerRecords<String, User> poll = consumer.poll(Duration.ofMillis(1000));
//            for (ConsumerRecord record : poll) {
//                logger.info(record.topic() + " - " + record.partition() + " - " + record.value());
//            }
//        }
//    }
//}
