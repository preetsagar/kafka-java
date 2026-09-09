import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Fetch response (API key 1), v16.
 *
 * <p>For each requested topic: if its id is unknown to {@link ClusterMetadata} the partition comes
 * back {@link #ERROR_UNKNOWN_TOPIC_ID}; otherwise the partition's on-disk log segment is streamed
 * back verbatim as the record set.
 *
 * <p>Returned bytes are the response header (v1: {@code correlation_id + TAG_BUFFER}) plus body.
 */
final class FetchResponse {

    static final short ERROR_NONE = 0;
    static final short ERROR_UNKNOWN_TOPIC_ID = 100; // UNKNOWN_TOPIC_ID
    private static final int PREFERRED_READ_REPLICA = -1;
    private static final String LOG_FILE_NAME = "00000000000000000000.log";

    private final Path logDir;

    FetchResponse(Path logDir) {
        this.logDir = logDir;
    }

    byte[] build(RequestHeader header, FetchRequest request, ClusterMetadata metadata) {
        List<ResolvedTopic> resolved = new ArrayList<>();
        int recordBytes = 0;
        for (FetchRequest.Topic topic : request.topics()) {
            ResolvedTopic resolvedTopic = resolve(topic, metadata);
            resolved.add(resolvedTopic);
            for (byte[] records : resolvedTopic.records()) {
                recordBytes += records.length;
            }
        }

        ByteBuffer buf = ByteBuffer.allocate(64 + resolved.size() * 32 + request.topics().size() * 64 + recordBytes
                + resolved.stream().mapToInt(t -> t.partitionIds().size() * 64).sum());

        buf.putInt(header.correlationId()); // response header v1
        Protocol.writeEmptyTagBuffer(buf);

        buf.putInt(0);            // throttle_time_ms
        buf.putShort(ERROR_NONE); // top-level error_code
        buf.putInt(0);            // session_id

        Protocol.writeUnsignedVarint(buf, resolved.size() + 1); // topics COMPACT_ARRAY
        for (ResolvedTopic topic : resolved) {
            writeTopic(buf, topic);
        }

        Protocol.writeEmptyTagBuffer(buf); // body tag buffer
        return Arrays.copyOf(buf.array(), buf.position());
    }

    private void writeTopic(ByteBuffer buf, ResolvedTopic topic) {
        buf.put(topic.uuid());
        Protocol.writeUnsignedVarint(buf, topic.partitionIds().size() + 1); // partitions COMPACT_ARRAY
        for (int i = 0; i < topic.partitionIds().size(); i++) {
            writePartition(buf, topic.partitionIds().get(i), topic.known(), topic.records().get(i));
        }
        Protocol.writeEmptyTagBuffer(buf); // topic tag buffer
    }

    private void writePartition(ByteBuffer buf, int partitionId, boolean known, byte[] records) {
        buf.putInt(partitionId);
        buf.putShort(known ? ERROR_NONE : ERROR_UNKNOWN_TOPIC_ID);
        buf.putLong(0); // high_watermark
        buf.putLong(0); // last_stable_offset
        buf.putLong(0); // log_start_offset
        Protocol.writeUnsignedVarint(buf, 1); // aborted_transactions: empty
        buf.putInt(PREFERRED_READ_REPLICA);

        if (records.length == 0) {
            Protocol.writeUnsignedVarint(buf, 0); // records: empty
        } else {
            Protocol.writeUnsignedVarint(buf, records.length + 1); // COMPACT_RECORDS length
            buf.put(records);
        }
        Protocol.writeEmptyTagBuffer(buf); // partition tag buffer
    }

    private ResolvedTopic resolve(FetchRequest.Topic topic, ClusterMetadata metadata) {
        ClusterMetadata.Topic known = metadata.topicByUuid(topic.uuid());
        List<byte[]> records = new ArrayList<>();
        for (int partitionId : topic.partitionIds()) {
            records.add(known == null ? new byte[0] : readPartitionLog(known.name(), partitionId));
        }
        return new ResolvedTopic(topic.uuid(), topic.partitionIds(), known != null, records);
    }

    private byte[] readPartitionLog(String topicName, int partitionId) {
        Path path = logDir.resolve(topicName + "-" + partitionId).resolve(LOG_FILE_NAME);
        if (!Files.isReadable(path)) {
            return new byte[0];
        }
        try {
            return Files.readAllBytes(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private record ResolvedTopic(byte[] uuid, List<Integer> partitionIds, boolean known, List<byte[]> records) {}
}
