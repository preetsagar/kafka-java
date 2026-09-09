import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * Produce response (API key 0), v11.
 *
 * <p>A partition targets a known topic/partition iff {@link ClusterMetadata} knows it; those get the
 * request's record-batch bytes appended to their on-disk log and a success entry, the rest get
 * {@link #ERROR_UNKNOWN_TOPIC_OR_PARTITION}.
 *
 * <p>Returned bytes are the response header (v1: {@code correlation_id + TAG_BUFFER}) plus body.
 */
final class ProduceResponse {

    static final short ERROR_NONE = 0;
    static final short ERROR_UNKNOWN_TOPIC_OR_PARTITION = 3;
    private static final long LOG_APPEND_TIME_UNSET = -1;
    private static final String LOG_FILE_NAME = "00000000000000000000.log";

    private final Path logDir;

    ProduceResponse(Path logDir) {
        this.logDir = logDir;
    }

    byte[] build(RequestHeader header, ProduceRequest request, ClusterMetadata metadata) {
        int capacity = 64;
        for (ProduceRequest.Topic topic : request.topics()) {
            capacity += 32 + topic.partitions().size() * 64;
        }
        ByteBuffer buf = ByteBuffer.allocate(capacity);

        buf.putInt(header.correlationId()); // response header v1
        Protocol.writeEmptyTagBuffer(buf);

        Protocol.writeUnsignedVarint(buf, request.topics().size() + 1); // topics COMPACT_ARRAY
        for (ProduceRequest.Topic topic : request.topics()) {
            writeTopic(buf, topic, metadata);
        }

        buf.putInt(0);                    // throttle_time_ms
        Protocol.writeEmptyTagBuffer(buf); // body tag buffer
        return Arrays.copyOf(buf.array(), buf.position());
    }

    private void writeTopic(ByteBuffer buf, ProduceRequest.Topic topic, ClusterMetadata metadata) {
        Protocol.writeCompactString(buf, topic.name());
        ClusterMetadata.Topic known = metadata.topic(topic.name());

        Protocol.writeUnsignedVarint(buf, topic.partitions().size() + 1); // partitions COMPACT_ARRAY
        for (ProduceRequest.Partition partition : topic.partitions()) {
            writePartition(buf, topic.name(), partition, partitionExists(known, partition.index()));
        }
        Protocol.writeEmptyTagBuffer(buf); // topic tag buffer
    }

    private void writePartition(ByteBuffer buf, String topicName, ProduceRequest.Partition partition, boolean valid) {
        long offset = valid ? append(topicName, partition) : -1;

        buf.putInt(partition.index());
        buf.putShort(valid ? ERROR_NONE : ERROR_UNKNOWN_TOPIC_OR_PARTITION);
        buf.putLong(offset);                 // base_offset
        buf.putLong(LOG_APPEND_TIME_UNSET);  // log_append_time_ms
        buf.putLong(valid ? 0 : -1);         // log_start_offset
        Protocol.writeUnsignedVarint(buf, 1); // record_errors: empty
        Protocol.writeUnsignedVarint(buf, 0); // error_message: null
        Protocol.writeEmptyTagBuffer(buf);   // partition tag buffer
    }

    private static boolean partitionExists(ClusterMetadata.Topic topic, int partitionId) {
        return topic != null && topic.partitions().stream().anyMatch(p -> p.id() == partitionId);
    }

    /** Writes the record-batch bytes to the partition's log segment; returns the base offset (0). */
    private long append(String topicName, ProduceRequest.Partition partition) {
        Path dir = logDir.resolve(topicName + "-" + partition.index());
        try {
            Files.createDirectories(dir);
            Files.write(dir.resolve(LOG_FILE_NAME), partition.records());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return 0;
    }
}
