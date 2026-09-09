import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.util.List;
import org.junit.jupiter.api.Test;

class DescribeTopicPartitionsResponseTest {

    private static final RequestHeader HEADER = new RequestHeader((short) 75, (short) 0, 7, "client");

    @Test
    void reportsUnknownTopicWithNilIdAndNoPartitions() {
        byte[] response = DescribeTopicPartitionsResponse.build(
                HEADER, new DescribeTopicPartitionsRequest(List.of("orders")));

        ByteBuffer buf = ByteBuffer.wrap(response);
        assertEquals(7, buf.getInt());              // correlation_id
        assertEquals(0, buf.get());                 // header TAG_BUFFER
        assertEquals(0, buf.getInt());              // throttle_time_ms
        assertEquals(2, buf.get());                 // topics array length (1 + 1)

        assertEquals((short) 3, buf.getShort());    // UNKNOWN_TOPIC_OR_PARTITION
        assertEquals(7, buf.get());                 // topic name length (6 + 1)
        byte[] name = new byte[6];
        buf.get(name);
        assertArrayEquals("orders".getBytes(), name);
        byte[] topicId = new byte[16];
        buf.get(topicId);
        assertArrayEquals(new byte[16], topicId);   // nil UUID
        assertEquals(0, buf.get());                 // is_internal
        assertEquals(1, buf.get());                 // partitions array: empty
        assertEquals(0x00000DF8, buf.getInt());     // authorized operations
        assertEquals(0, buf.get());                 // topic TAG_BUFFER

        assertEquals((byte) 0xFF, buf.get());       // next_cursor: null
        assertEquals(0, buf.get());                 // response TAG_BUFFER
        assertEquals(0, buf.remaining());
    }

    @Test
    void emitsOneEntryPerRequestedTopic() {
        byte[] response = DescribeTopicPartitionsResponse.build(
                HEADER, new DescribeTopicPartitionsRequest(List.of("a", "b", "c")));
        assertEquals(4, ByteBuffer.wrap(response).get(9)); // topics array length (3 + 1)
    }
}
