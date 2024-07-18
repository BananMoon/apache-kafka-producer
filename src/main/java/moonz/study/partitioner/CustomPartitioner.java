package moonz.study.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

/**
 * Message key에 따라 파티션 번호를 지정하는 커스텀 Partitioner
 */
public class CustomPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (keyBytes == null) {
            throw new InvalidRecordException("메시지 키가 필요합니다.");
        }
        if (((String) key).equals("Pangyo")) {
            return 0;
        }
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int partitionSize = partitions.size();

        // 해쉬값 변환하여 동일한 메시지 키는 동일한 파티션에 저장되도록 한다.
        return Utils.toPositive(Utils.murmur2(keyBytes)) % partitionSize;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
