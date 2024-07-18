package moonz.study.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 토픽-파티션번호@오프셋번호
 * acks=0 : 파티션에 저장됐는지 응답 확인 없이 성공 처리하므로 덜 의미있는 응답을 받음. -> 오프셋 번호 : -1로 나타남.
 * acks=1 : 리더 파티션에 저장됨을 보장하고 응답받도록 설정.
 */
public class ProducerWithSyncCallback {
    private static final Logger logger = LoggerFactory.getLogger(ProducerWithSyncCallback.class);
    private static final String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static final String TOPIC_NAME = "test";
    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        configs.put(ProducerConfig.ACKS_CONFIG, "0"); // 어떤 오프셋 번호에 저장됐는지 확인 어려운 설정 상태.

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "Pangyo", "Pangyo");

        try {
            RecordMetadata metaData = producer.send(record).get();
            logger.info(metaData.toString());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            producer.flush();
            producer.close();
        }

    }

}
