import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * DescribeTopicPartitions request (API key 75), v0.
 *
 * <p>Body: {@code topics COMPACT_ARRAY[{ name COMPACT_STRING, TAG_BUFFER }],
 * response_partition_limit INT32, cursor (nullable), TAG_BUFFER}. Only the topic names
 * matter to this broker so far.
 */
record DescribeTopicPartitionsRequest(List<String> topicNames) {

    /** Parses the body from {@code buf}, which must be positioned just past the request header. */
    static DescribeTopicPartitionsRequest parse(ByteBuffer buf) {
        int topicCount = Protocol.readUnsignedVarint(buf) - 1; // COMPACT_ARRAY length is N+1
        List<String> names = new ArrayList<>(Math.max(topicCount, 0));
        for (int i = 0; i < topicCount; i++) {
            names.add(Protocol.readCompactString(buf));
            Protocol.skipTagBuffer(buf);
        }
        return new DescribeTopicPartitionsRequest(names);
    }
}
