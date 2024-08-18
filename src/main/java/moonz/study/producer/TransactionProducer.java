package moonz.study.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * 트랜잭션 프로듀서.
 * 다수의 데이터를 발송한 뒤 커밋 데이터를 보내
 * 여러 데이터들이 한번에 처리되거나 처리되지 않도록 한다.
 */
public class TransactionProducer {

    private static final Logger logger = LoggerFactory.getLogger(TransactionProducer.class);
    private static final String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static final String TOPIC_NAME = "test";

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID());  // 프로듀서 별 고유한 ID 값을 사용해야 함.
        KafkaProducer<String, String> producer = new KafkaProducer<>(config);

        producer.initTransactions();
        producer.beginTransaction();

        try {
            producer.send(new ProducerRecord<>(TOPIC_NAME, "전달하는 메세지 값"));

            TimeUnit.SECONDS.sleep(30);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            // 30초 후 커밋한다. 그때 consumer가 poll하는지 확인한다.
            producer.commitTransaction();
            producer.close();
        }
    }
}
