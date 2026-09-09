import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class ApiVersionsResponseTest {

    @Test
    void encodesApiVersionsV4BodyForSupportedVersion() {
        byte[] response = ApiVersionsResponse.build(new RequestHeader((short) 18, (short) 4, 311));

        byte[] expected = ByteBuffer.allocate(26)
                .putInt(311)            // correlation_id
                .putShort((short) 0)    // error_code
                .put((byte) 3)          // api_keys array length (N+1), 2 entries
                .putShort((short) 18)   // ApiVersions
                .putShort((short) 0)
                .putShort((short) 4)
                .put((byte) 0)
                .putShort((short) 75)   // DescribeTopicPartitions
                .putShort((short) 0)
                .putShort((short) 0)
                .put((byte) 0)
                .putInt(0)              // throttle_time_ms
                .put((byte) 0)          // response tag buffer
                .array();

        assertArrayEquals(expected, response);
    }

    @Test
    void advertisesDescribeTopicPartitions() {
        byte[] response = ApiVersionsResponse.build(new RequestHeader((short) 18, (short) 4, 1));
        ByteBuffer buf = ByteBuffer.wrap(response);
        buf.position(7); // skip correlation_id, error_code, array length

        buf.position(buf.position() + 7); // skip the ApiVersions entry
        assertEquals((short) 75, buf.getShort());
        assertEquals((short) 0, buf.getShort()); // min_version
        assertEquals((short) 0, buf.getShort()); // max_version
    }

    @Test
    void reportsUnsupportedVersionButStillReturnsApiKeys() {
        byte[] response = ApiVersionsResponse.build(new RequestHeader((short) 18, (short) 9, 1));
        ByteBuffer buf = ByteBuffer.wrap(response);

        assertEquals(1, buf.getInt());                  // correlation_id echoed
        assertEquals((short) 35, buf.getShort());       // UNSUPPORTED_VERSION
        assertEquals((byte) 3, buf.get());              // still advertises both api keys
        assertEquals((short) 18, buf.getShort());
    }

    @Test
    void versionZeroIsSupported() {
        byte[] response = ApiVersionsResponse.build(new RequestHeader((short) 18, (short) 0, 1));
        assertEquals((short) 0, ByteBuffer.wrap(response).getShort(4)); // error_code at offset 4
    }
}
