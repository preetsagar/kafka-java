import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Produce request (API key 0), v11.
 *
 * <p>Body: {@code transactional_id, acks, timeout_ms, topics COMPACT_ARRAY[{ name COMPACT_STRING,
 * partitions COMPACT_ARRAY[{ index INT32, records COMPACT_RECORDS, TAG_BUFFER }], TAG_BUFFER }],
 * TAG_BUFFER}. Only the topic/partition targets and the raw record-batch bytes matter here.
 */
record ProduceRequest(List<Topic> topics) {

    record Topic(String name, List<Partition> partitions) {}

    /** {@code records} holds the raw record-batch bytes as they should land on disk. */
    record Partition(int index, byte[] records) {}

    /** Parses the body from {@code buf}, which must be positioned just past the request header. */
    static ProduceRequest parse(ByteBuffer buf) {
        skipCompactString(buf); // transactional_id (nullable)
        buf.getShort();         // acks
        buf.getInt();           // timeout_ms

        int topicCount = Protocol.readCompactLength(buf);
        List<Topic> topics = new ArrayList<>(Math.max(topicCount, 0));
        for (int i = 0; i < topicCount; i++) {
            String name = Protocol.readCompactString(buf);

            int partitionCount = Protocol.readCompactLength(buf);
            List<Partition> partitions = new ArrayList<>(Math.max(partitionCount, 0));
            for (int p = 0; p < partitionCount; p++) {
                int index = buf.getInt();
                int recordsLength = Math.max(Protocol.readUnsignedVarint(buf) - 1, 0); // COMPACT_RECORDS
                byte[] records = new byte[recordsLength];
                buf.get(records);
                Protocol.skipTagBuffer(buf);
                partitions.add(new Partition(index, records));
            }
            Protocol.skipTagBuffer(buf);
            topics.add(new Topic(name, partitions));
        }
        return new ProduceRequest(topics);
    }

    private static void skipCompactString(ByteBuffer buf) {
        int length = Protocol.readUnsignedVarint(buf) - 1;
        if (length > 0) {
            buf.position(buf.position() + length);
        }
    }
}
