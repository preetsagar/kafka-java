import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * DescribeTopicPartitions response (API key 75), v0.
 *
 * <p>Known topics (present in {@link ClusterMetadata}) come back with error code 0, their real
 * UUID and one entry per partition; unknown topics come back {@link #ERROR_UNKNOWN_TOPIC} with a
 * nil UUID and no partitions. Topics are returned sorted by name, which is the order the tester
 * asserts against.
 *
 * <p>Returned bytes are the response header (v1: {@code correlation_id + TAG_BUFFER}) plus body.
 */
final class DescribeTopicPartitionsResponse {

    static final short ERROR_NONE = 0;
    static final short ERROR_UNKNOWN_TOPIC = 3; // UNKNOWN_TOPIC_OR_PARTITION
    static final int AUTHORIZED_OPERATIONS = 0x00000DF8; // READ..DESCRIBE_CONFIGS
    private static final byte[] NIL_UUID = new byte[16];

    private DescribeTopicPartitionsResponse() {}

    static byte[] build(RequestHeader header, DescribeTopicPartitionsRequest request, ClusterMetadata metadata) {
        List<String> names = new ArrayList<>(request.topicNames());
        names.sort(null);

        ByteBuffer buf = ByteBuffer.allocate(64 + names.size() * 256);
        buf.putInt(header.correlationId()); // response header v1
        Protocol.writeEmptyTagBuffer(buf);

        buf.putInt(0); // throttle_time_ms

        Protocol.writeUnsignedVarint(buf, names.size() + 1); // topics COMPACT_ARRAY
        for (String name : names) {
            writeTopic(buf, name, metadata.topic(name));
        }

        buf.put((byte) 0xFF); // next_cursor: null
        Protocol.writeEmptyTagBuffer(buf);

        return Arrays.copyOf(buf.array(), buf.position());
    }

    private static void writeTopic(ByteBuffer buf, String name, ClusterMetadata.Topic topic) {
        boolean known = topic != null;
        buf.putShort(known ? ERROR_NONE : ERROR_UNKNOWN_TOPIC);
        Protocol.writeCompactNullableString(buf, name);
        buf.put(known ? topic.uuid() : NIL_UUID);
        buf.put((byte) 0); // is_internal

        List<ClusterMetadata.Partition> partitions = known ? topic.partitions() : List.of();
        Protocol.writeUnsignedVarint(buf, partitions.size() + 1); // partitions COMPACT_ARRAY
        for (ClusterMetadata.Partition partition : partitions) {
            writePartition(buf, partition);
        }

        buf.putInt(AUTHORIZED_OPERATIONS);
        Protocol.writeEmptyTagBuffer(buf);
    }

    private static void writePartition(ByteBuffer buf, ClusterMetadata.Partition partition) {
        buf.putShort(ERROR_NONE);
        buf.putInt(partition.id());
        buf.putInt(partition.leaderId());
        buf.putInt(partition.leaderEpoch());
        writeInt32Array(buf, partition.replicas());
        writeInt32Array(buf, partition.isr());
        writeInt32Array(buf, List.of()); // eligible leader replicas
        writeInt32Array(buf, List.of()); // last known ELR
        writeInt32Array(buf, List.of()); // offline replicas
        Protocol.writeEmptyTagBuffer(buf);
    }

    private static void writeInt32Array(ByteBuffer buf, List<Integer> values) {
        Protocol.writeUnsignedVarint(buf, values.size() + 1);
        values.forEach(buf::putInt);
    }
}
