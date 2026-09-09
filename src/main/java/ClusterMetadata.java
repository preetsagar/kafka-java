import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The broker's view of cluster metadata, read from the KRaft {@code __cluster_metadata} log.
 *
 * <p>The log is a sequence of record batches; each record's value is a metadata payload
 * ({@code frame_version, type, version, ...}). We only care about topic records (type 2)
 * and partition records (type 3); every other type is skipped via the record length.
 */
final class ClusterMetadata {

    static final Path DEFAULT_LOG_PATH =
            Path.of("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log");

    private static final int TYPE_TOPIC = 2;
    private static final int TYPE_PARTITION = 3;

    record Partition(int id, int leaderId, int leaderEpoch, List<Integer> replicas, List<Integer> isr) {}

    record Topic(String name, byte[] uuid, List<Partition> partitions) {}

    private final Map<String, Topic> topicsByName;

    private ClusterMetadata(Map<String, Topic> topicsByName) {
        this.topicsByName = topicsByName;
    }

    static ClusterMetadata empty() {
        return new ClusterMetadata(Map.of());
    }

    /** Loads metadata from {@code path}, or returns an empty view if the log is missing. */
    static ClusterMetadata load(Path path) {
        if (!Files.isReadable(path)) {
            return empty();
        }
        try {
            return parse(ByteBuffer.wrap(Files.readAllBytes(path)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    Topic topic(String name) {
        return topicsByName.get(name);
    }

    static ClusterMetadata parse(ByteBuffer log) {
        Map<String, byte[]> uuidByName = new LinkedHashMap<>();
        Map<String, List<Partition>> partitionsByUuidHex = new LinkedHashMap<>();

        while (log.hasRemaining()) {
            parseRecordBatch(log, uuidByName, partitionsByUuidHex);
        }

        Map<String, Topic> topics = new LinkedHashMap<>();
        uuidByName.forEach((name, uuid) -> {
            List<Partition> partitions =
                    partitionsByUuidHex.getOrDefault(hex(uuid), List.of());
            partitions.sort((a, b) -> Integer.compare(a.id(), b.id()));
            topics.put(name, new Topic(name, uuid, partitions));
        });
        return new ClusterMetadata(topics);
    }

    private static void parseRecordBatch(
            ByteBuffer log, Map<String, byte[]> uuidByName, Map<String, List<Partition>> partitionsByUuidHex) {
        log.getLong(); // base offset
        int batchLength = log.getInt();
        int batchEnd = log.position() + batchLength;

        log.getInt();   // partition leader epoch
        log.get();      // magic
        log.getInt();   // crc
        log.getShort(); // attributes
        log.getInt();   // last offset delta
        log.getLong();  // first timestamp
        log.getLong();  // max timestamp
        log.getLong();  // producer id
        log.getShort(); // producer epoch
        log.getInt();   // base sequence
        int recordCount = log.getInt();

        for (int i = 0; i < recordCount; i++) {
            int recordLength = (int) Protocol.readSignedVarint(log);
            int recordEnd = log.position() + recordLength;
            parseRecordValue(log, uuidByName, partitionsByUuidHex);
            log.position(recordEnd);
        }
        log.position(batchEnd);
    }

    private static void parseRecordValue(
            ByteBuffer log, Map<String, byte[]> uuidByName, Map<String, List<Partition>> partitionsByUuidHex) {
        log.get(); // record attributes
        Protocol.readSignedVarint(log); // timestamp delta
        Protocol.readSignedVarint(log); // offset delta

        int keyLength = (int) Protocol.readSignedVarint(log);
        if (keyLength > 0) {
            log.position(log.position() + keyLength);
        }
        int valueLength = (int) Protocol.readSignedVarint(log);
        if (valueLength <= 0) {
            return;
        }
        ByteBuffer value = log.slice();
        value.limit(valueLength);

        value.get(); // frame version
        int type = value.get();
        value.get(); // record version

        if (type == TYPE_TOPIC) {
            String name = Protocol.readCompactString(value);
            byte[] uuid = new byte[16];
            value.get(uuid);
            uuidByName.put(name, uuid);
        } else if (type == TYPE_PARTITION) {
            int partitionId = value.getInt();
            byte[] topicUuid = new byte[16];
            value.get(topicUuid);
            List<Integer> replicas = readInt32Array(value);
            List<Integer> isr = readInt32Array(value);
            readInt32Array(value); // removing replicas
            readInt32Array(value); // adding replicas
            int leader = value.getInt();
            int leaderEpoch = value.getInt();
            partitionsByUuidHex
                    .computeIfAbsent(hex(topicUuid), k -> new ArrayList<>())
                    .add(new Partition(partitionId, leader, leaderEpoch, replicas, isr));
        }
    }

    private static List<Integer> readInt32Array(ByteBuffer buf) {
        int count = Protocol.readCompactLength(buf);
        List<Integer> values = new ArrayList<>(Math.max(count, 0));
        for (int i = 0; i < count; i++) {
            values.add(buf.getInt());
        }
        return values;
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }
}
