import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class ProduceRequestTest {

    @Test
    void parsesTopicPartitionAndRecordBytes() {
        byte[] recordBatch = {1, 2, 3, 4, 5};

        ByteBuffer buf = ByteBuffer.allocate(128);
        Protocol.writeUnsignedVarint(buf, 0);   // transactional_id: null
        buf.putShort((short) -1);               // acks
        buf.putInt(30000);                      // timeout_ms
        Protocol.writeUnsignedVarint(buf, 2);   // 1 topic
        Protocol.writeCompactString(buf, "orders");
        Protocol.writeUnsignedVarint(buf, 2);   // 1 partition
        buf.putInt(3);                          // partition index
        Protocol.writeUnsignedVarint(buf, recordBatch.length + 1); // COMPACT_RECORDS
        buf.put(recordBatch);
        buf.put((byte) 0);                      // partition tag buffer
        buf.put((byte) 0);                      // topic tag buffer
        buf.put((byte) 0);                      // body tag buffer
        buf.flip();

        ProduceRequest request = ProduceRequest.parse(buf);

        assertEquals(1, request.topics().size());
        ProduceRequest.Topic topic = request.topics().getFirst();
        assertEquals("orders", topic.name());
        assertEquals(3, topic.partitions().getFirst().index());
        assertArrayEquals(recordBatch, topic.partitions().getFirst().records());
    }

    @Test
    void parsesTransactionalId() {
        ByteBuffer buf = ByteBuffer.allocate(64);
        Protocol.writeCompactString(buf, "txn-1"); // transactional_id present
        buf.putShort((short) -1);
        buf.putInt(30000);
        Protocol.writeUnsignedVarint(buf, 1);       // no topics
        buf.put((byte) 0);
        buf.flip();

        assertEquals(0, ProduceRequest.parse(buf).topics().size());
    }
}
