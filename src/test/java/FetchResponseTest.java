import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FetchResponseTest {

    private static final RequestHeader HEADER = new RequestHeader((short) 1, (short) 16, 99, "client");

    @Test
    void emptyTopicsRequestReturnsEmptyBody(@TempDir Path logDir) {
        byte[] response = new FetchResponse(logDir)
                .build(HEADER, new FetchRequest(List.of()), ClusterMetadata.empty());

        ByteBuffer buf = ByteBuffer.wrap(response);
        assertEquals(99, buf.getInt());          // correlation_id
        assertEquals(0, buf.get());              // header tag buffer
        assertEquals(0, buf.getInt());           // throttle_time_ms
        assertEquals((short) 0, buf.getShort()); // error_code
        assertEquals(0, buf.getInt());           // session_id
        assertEquals(1, buf.get());              // topics array: empty
        assertEquals(0, buf.get());              // body tag buffer
        assertEquals(0, buf.remaining());
    }

    @Test
    void unknownTopicIdYieldsErrorCode100(@TempDir Path logDir) {
        byte[] uuid = new byte[16];
        uuid[0] = 0x42;
        FetchRequest request = new FetchRequest(List.of(new FetchRequest.Topic(uuid, List.of(0))));

        byte[] response = new FetchResponse(logDir).build(HEADER, request, ClusterMetadata.empty());

        ByteBuffer buf = ByteBuffer.wrap(response);
        buf.position(16); // header(5) + throttle(4) + error(2) + session(4) + topics length(1)
        byte[] echoed = new byte[16];
        buf.get(echoed);
        assertArrayEquals(uuid, echoed);
        assertEquals(2, buf.get());              // partitions array: 1 entry
        assertEquals(0, buf.getInt());           // partition index
        assertEquals((short) 100, buf.getShort()); // UNKNOWN_TOPIC_ID
    }

    @Test
    void knownTopicStreamsPartitionLogVerbatim(@TempDir Path logDir) throws IOException {
        ClusterMetadata metadata = ClusterMetadata.parse(Fixtures.clusterMetadataLog());
        byte[] fooUuid = metadata.topic("foo").uuid();
        byte[] segment = Fixtures.partitionLog();

        Path partitionDir = logDir.resolve("foo-0");
        Files.createDirectories(partitionDir);
        Files.write(partitionDir.resolve("00000000000000000000.log"), segment);

        FetchRequest request = new FetchRequest(List.of(new FetchRequest.Topic(fooUuid, List.of(0))));
        byte[] response = new FetchResponse(logDir).build(HEADER, request, metadata);

        ByteBuffer buf = ByteBuffer.wrap(response);
        buf.position(16 + 16); // body prefix + echoed uuid
        assertEquals(2, buf.get());              // partitions array: 1 entry
        assertEquals(0, buf.getInt());           // partition index
        assertEquals((short) 0, buf.getShort()); // no error
        buf.position(buf.position() + 8 + 8 + 8); // high watermark, last stable, log start
        assertEquals(1, buf.get());              // aborted_transactions: empty
        assertEquals(-1, buf.getInt());          // preferred_read_replica
        int recordsLength = Protocol.readUnsignedVarint(buf) - 1;
        assertEquals(segment.length, recordsLength);
        byte[] records = new byte[recordsLength];
        buf.get(records);
        assertArrayEquals(segment, records);
    }
}
