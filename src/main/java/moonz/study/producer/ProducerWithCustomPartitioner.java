package moonz.study.producer;

import moonz.study.partitioner.CustomPartitioner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 커스텀 파티셔너를 지정한 Producer
 */
public class ProducerWithCustomPartitioner {
    private static final Logger logger = LoggerFactory.getLogger(ProducerWithCustomPartitioner.class);

    private static final String BOOTSTRAP_SERVERS = "my-kafka:9092";
    private static final String TOPIC_NAME = "test";
    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        int partitionNo = 0;

        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, partitionNo, "Pangyo", "PangyoValue");

        try {
            RecordMetadata metaData = producer.send(record).get();
            logger.info(metaData.toString());        // 계속 파티션 0번으로 저장되는 것 확인.

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            producer.flush();
            producer.close();
        }

        producer.flush();
        producer.close();
    }
}