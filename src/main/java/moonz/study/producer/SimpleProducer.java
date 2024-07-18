package moonz.study.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 메세지 value 전송하는 Producer
 */
public class SimpleProducer {
    private static final Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private static final String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static final String TOPIC_NAME = "test";

    public static void main(String[] args) {

        // 브로커에 데이터 1개 전송하는 로직 작성.
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        String messageValue = "testMessageValue";
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);
        producer.send(record);  // 바로 전송되지는 않음. 내부에 어큐뮬레이터 존재.
        logger.info("record: {}", record);

        producer.flush();   // 강제로 어큐뮬레이터에 있는 데이터를 전송시키는 명령어.
        producer.close();
    }
}
