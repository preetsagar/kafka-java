import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.Test;

class FetchRequestTest {

    @Test
    void parsesTopicUuidsAndPartitionIds() {
        byte[] uuid = new byte[16];
        uuid[0] = 0x11;
        uuid[15] = 0x2a;

        ByteBuffer buf = ByteBuffer.allocate(128);
        buf.putInt(500);        // max_wait_ms
        buf.putInt(1);          // min_bytes
        buf.putInt(Integer.MAX_VALUE); // max_bytes
        buf.put((byte) 0);      // isolation_level
        buf.putInt(0);          // session_id
        buf.putInt(0);          // session_epoch
        Protocol.writeUnsignedVarint(buf, 2); // 1 topic
        buf.put(uuid);
        Protocol.writeUnsignedVarint(buf, 2); // 1 partition
        buf.putInt(7);          // partition id
        buf.putInt(-1);         // current_leader_epoch
        buf.putLong(0);         // fetch_offset
        buf.putInt(-1);         // last_fetched_offset
        buf.putLong(-1);        // log_start_offset
        buf.putInt(Integer.MAX_VALUE); // partition_max_bytes
        buf.put((byte) 0);      // partition tag buffer
        buf.put((byte) 0);      // topic tag buffer
        Protocol.writeUnsignedVarint(buf, 1); // forgotten topics: empty
        Protocol.writeUnsignedVarint(buf, 1); // rack id: empty compact string
        buf.put((byte) 0);      // body tag buffer
        buf.flip();

        FetchRequest request = FetchRequest.parse(buf);

        assertEquals(1, request.topics().size());
        assertArrayEquals(uuid, request.topics().getFirst().uuid());
        assertEquals(List.of(7), request.topics().getFirst().partitionIds());
    }

    @Test
    void parsesEmptyTopics() {
        ByteBuffer buf = ByteBuffer.allocate(32);
        buf.putInt(500).putInt(1).putInt(Integer.MAX_VALUE).put((byte) 0).putInt(0).putInt(0);
        Protocol.writeUnsignedVarint(buf, 1); // topics: empty
        Protocol.writeUnsignedVarint(buf, 1); // forgotten topics: empty
        Protocol.writeUnsignedVarint(buf, 1); // rack id
        buf.put((byte) 0);
        buf.flip();

        assertTrue(FetchRequest.parse(buf).topics().isEmpty());
    }
}
