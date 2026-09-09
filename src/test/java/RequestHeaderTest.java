import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class RequestHeaderTest {

    @Test
    void parsesHeaderAndLeavesBufferAtBody() {
        byte[] clientId = "adminclient-1".getBytes(UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(64)
                .putShort((short) 75)              // api_key
                .putShort((short) 0)               // api_version
                .putInt(0x6f7f2d1e)                // correlation_id
                .putShort((short) clientId.length) // client_id length
                .put(clientId)
                .put((byte) 0)                     // header TAG_BUFFER
                .putInt(0xdeadbeef);               // start of body
        buf.flip();

        RequestHeader header = RequestHeader.parse(buf);

        assertEquals((short) 75, header.apiKey());
        assertEquals((short) 0, header.apiVersion());
        assertEquals(0x6f7f2d1e, header.correlationId());
        assertEquals("adminclient-1", header.clientId());
        assertEquals(0xdeadbeef, buf.getInt()); // buffer now sits at the body
    }

    @Test
    void handlesNullClientId() {
        ByteBuffer buf = ByteBuffer.allocate(16)
                .putShort((short) 18)
                .putShort((short) 4)
                .putInt(7)
                .putShort((short) -1) // null client_id
                .put((byte) 0);
        buf.flip();

        RequestHeader header = RequestHeader.parse(buf);

        assertNull(header.clientId());
        assertEquals(7, header.correlationId());
    }
}
