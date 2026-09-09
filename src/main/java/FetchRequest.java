import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Fetch request (API key 1), v16.
 *
 * <p>Only the requested topic ids and partition ids matter to this broker; the rest of the body
 * (fetch offsets, byte limits, forgotten topics, rack id) is parsed but discarded.
 */
record FetchRequest(List<Topic> topics) {

    record Topic(byte[] uuid, List<Integer> partitionIds) {}

    /** Parses the body from {@code buf}, which must be positioned just past the request header. */
    static FetchRequest parse(ByteBuffer buf) {
        buf.getInt();  // max_wait_ms
        buf.getInt();  // min_bytes
        buf.getInt();  // max_bytes
        buf.get();     // isolation_level
        buf.getInt();  // session_id
        buf.getInt();  // session_epoch

        int topicCount = Protocol.readCompactLength(buf);
        List<Topic> topics = new ArrayList<>(Math.max(topicCount, 0));
        for (int i = 0; i < topicCount; i++) {
            byte[] uuid = new byte[16];
            buf.get(uuid);

            int partitionCount = Protocol.readCompactLength(buf);
            List<Integer> partitionIds = new ArrayList<>(Math.max(partitionCount, 0));
            for (int p = 0; p < partitionCount; p++) {
                partitionIds.add(buf.getInt()); // partition id
                buf.getInt();  // current_leader_epoch
                buf.getLong(); // fetch_offset
                buf.getInt();  // last_fetched_offset
                buf.getLong(); // log_start_offset
                buf.getInt();  // partition_max_bytes
                Protocol.skipTagBuffer(buf);
            }
            Protocol.skipTagBuffer(buf);
            topics.add(new Topic(uuid, partitionIds));
        }
        return new FetchRequest(topics);
    }
}
