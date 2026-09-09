import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class RequestHeaderTest {

    @Test
    void parsesApiKeyVersionAndCorrelationId() {
        byte[] message = ByteBuffer.allocate(12)
                .putShort((short) 18)   // api_key
                .putShort((short) 4)    // api_version
                .putInt(0x6f7f2d1e)     // correlation_id
                .putInt(0)              // trailing bytes ignored
                .array();

        RequestHeader header = RequestHeader.parse(message);

        assertEquals((short) 18, header.apiKey());
        assertEquals((short) 4, header.apiVersion());
        assertEquals(0x6f7f2d1e, header.correlationId());
    }

    @Test
    void rejectsTruncatedHeader() {
        assertThrows(IllegalArgumentException.class, () -> RequestHeader.parse(new byte[] {0, 18, 0, 4}));
    }
}
