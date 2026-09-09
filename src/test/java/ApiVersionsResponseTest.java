import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class ApiVersionsResponseTest {

    @Test
    void encodesApiVersionsV4BodyForSupportedVersion() {
        byte[] response = ApiVersionsResponse.build(new RequestHeader((short) 18, (short) 4, 311));

        byte[] expected = ByteBuffer.allocate(19)
                .putInt(311)            // correlation_id
                .putShort((short) 0)    // error_code
                .put((byte) 2)          // api_keys array length (N+1)
                .putShort((short) 18)   // api_key
                .putShort((short) 0)    // min_version
                .putShort((short) 4)    // max_version
                .put((byte) 0)          // entry tag buffer
                .putInt(0)              // throttle_time_ms
                .put((byte) 0)          // response tag buffer
                .array();

        assertArrayEquals(expected, response);
    }

    @Test
    void reportsUnsupportedVersionButStillReturnsApiKeyEntry() {
        byte[] response = ApiVersionsResponse.build(new RequestHeader((short) 18, (short) 9, 1));
        ByteBuffer buf = ByteBuffer.wrap(response);

        assertEquals(1, buf.getInt());                  // correlation_id echoed
        assertEquals((short) 35, buf.getShort());       // UNSUPPORTED_VERSION
        assertEquals((byte) 2, buf.get());              // still advertises one api key
        assertEquals((short) 18, buf.getShort());
    }

    @Test
    void versionZeroIsSupported() {
        byte[] response = ApiVersionsResponse.build(new RequestHeader((short) 18, (short) 0, 1));
        assertEquals((short) 0, ByteBuffer.wrap(response).getShort(4)); // error_code at offset 4
    }
}
