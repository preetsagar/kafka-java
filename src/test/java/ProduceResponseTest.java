import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ProduceResponseTest {

    private static final RequestHeader HEADER = new RequestHeader((short) 0, (short) 11, 55, "client");

    @Test
    void knownPartitionAppendsRecordsAndReportsSuccess(@TempDir Path logDir) {
        ClusterMetadata metadata = ClusterMetadata.parse(Fixtures.clusterMetadataLog());
        byte[] batch = Fixtures.partitionLog();
        ProduceRequest request = new ProduceRequest(List.of(
                new ProduceRequest.Topic("foo", List.of(new ProduceRequest.Partition(0, batch)))));

        byte[] response = new ProduceResponse(logDir).build(HEADER, request, metadata);

        Path written = logDir.resolve("foo-0").resolve("00000000000000000000.log");
        assertTrue(Files.exists(written));
        assertArrayEquals(batch, readAll(written));

        Partition partition = firstPartition(response);
        assertEquals(0, partition.index());
        assertEquals((short) 0, partition.errorCode());
        assertEquals(0, partition.baseOffset());
        assertEquals(-1, partition.logAppendTimeMs());
        assertEquals(0, partition.logStartOffset());
    }

    @Test
    void unknownTopicReportsErrorAndWritesNothing(@TempDir Path logDir) {
        ProduceRequest request = new ProduceRequest(List.of(
                new ProduceRequest.Topic("ghost", List.of(new ProduceRequest.Partition(0, new byte[] {9})))));

        byte[] response = new ProduceResponse(logDir).build(HEADER, request, ClusterMetadata.empty());

        assertFalse(Files.exists(logDir.resolve("ghost-0")));
        Partition partition = firstPartition(response);
        assertEquals((short) 3, partition.errorCode());
        assertEquals(-1, partition.baseOffset());
        assertEquals(-1, partition.logAppendTimeMs());
        assertEquals(-1, partition.logStartOffset());
    }

    @Test
    void knownTopicButMissingPartitionReportsError(@TempDir Path logDir) {
        ClusterMetadata metadata = ClusterMetadata.parse(Fixtures.clusterMetadataLog());
        ProduceRequest request = new ProduceRequest(List.of(
                new ProduceRequest.Topic("foo", List.of(new ProduceRequest.Partition(999, new byte[] {9})))));

        byte[] response = new ProduceResponse(logDir).build(HEADER, request, metadata);

        assertFalse(Files.exists(logDir.resolve("foo-999")));
        assertEquals((short) 3, firstPartition(response).errorCode());
    }

    private record Partition(int index, short errorCode, long baseOffset, long logAppendTimeMs, long logStartOffset) {}

    private static Partition firstPartition(byte[] response) {
        ByteBuffer buf = ByteBuffer.wrap(response);
        buf.position(5); // correlation_id + header tag buffer
        Protocol.readUnsignedVarint(buf); // topics array length
        Protocol.readCompactString(buf);  // topic name
        Protocol.readUnsignedVarint(buf); // partitions array length
        return new Partition(
                buf.getInt(), buf.getShort(), buf.getLong(), buf.getLong(), buf.getLong());
    }

    private static byte[] readAll(Path path) {
        try {
            return Files.readAllBytes(path);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
