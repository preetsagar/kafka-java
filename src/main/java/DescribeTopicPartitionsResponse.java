import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * DescribeTopicPartitions response (API key 75), v0.
 *
 * <p>Every requested topic is currently unknown, so each entry reports
 * {@link #ERROR_UNKNOWN_TOPIC}, a nil topic id and no partitions.
 *
 * <p>Returned bytes are the response header (v1: {@code correlation_id + TAG_BUFFER}) plus body,
 * i.e. everything after the leading {@code message_size} field.
 */
final class DescribeTopicPartitionsResponse {

    static final short ERROR_UNKNOWN_TOPIC = 3; // UNKNOWN_TOPIC_OR_PARTITION
    static final int AUTHORIZED_OPERATIONS = 0x00000DF8; // READ..DESCRIBE_CONFIGS
    private static final byte[] NIL_UUID = new byte[16];

    private DescribeTopicPartitionsResponse() {}

    static byte[] build(RequestHeader header, DescribeTopicPartitionsRequest request) {
        int topicNameBytes = request.topicNames().stream().mapToInt(n -> n.getBytes().length).sum();
        ByteBuffer buf = ByteBuffer.allocate(64 + request.topicNames().size() * 32 + topicNameBytes);

        buf.putInt(header.correlationId()); // response header v1
        Protocol.writeEmptyTagBuffer(buf);

        buf.putInt(0); // throttle_time_ms

        Protocol.writeUnsignedVarint(buf, request.topicNames().size() + 1); // topics COMPACT_ARRAY
        for (String name : request.topicNames()) {
            buf.putShort(ERROR_UNKNOWN_TOPIC);
            Protocol.writeCompactNullableString(buf, name);
            buf.put(NIL_UUID); // topic_id
            buf.put((byte) 0); // is_internal
            Protocol.writeUnsignedVarint(buf, 1); // partitions COMPACT_ARRAY: empty
            buf.putInt(AUTHORIZED_OPERATIONS);
            Protocol.writeEmptyTagBuffer(buf);
        }

        buf.put((byte) 0xFF); // next_cursor: null
        Protocol.writeEmptyTagBuffer(buf);

        return Arrays.copyOf(buf.array(), buf.position());
    }
}
