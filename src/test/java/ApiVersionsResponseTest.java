import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class ApiVersionsResponseTest {

    @Test
    void encodesApiVersionsV4BodyForSupportedVersion() {
        byte[] response = ApiVersionsResponse.build(new RequestHeader((short) 18, (short) 4, 311, null));

        ByteBuffer expected = ByteBuffer.allocate(64);
        expected.putInt(311);           // correlation_id
        expected.putShort((short) 0);   // error_code
        expected.put((byte) 5);         // api_keys array length (N+1), 4 entries
        putEntry(expected, 0, 0, 11);   // Produce
        putEntry(expected, 1, 0, 16);   // Fetch
        putEntry(expected, 18, 0, 4);   // ApiVersions
        putEntry(expected, 75, 0, 0);   // DescribeTopicPartitions
        expected.putInt(0);             // throttle_time_ms
        expected.put((byte) 0);         // response tag buffer
        expected.flip();
        byte[] expectedBytes = new byte[expected.remaining()];
        expected.get(expectedBytes);

        assertArrayEquals(expectedBytes, response);
    }

    @Test
    void advertisesProduceFetchAndDescribeTopicPartitions() {
        int[] keys = apiKeys(ApiVersionsResponse.build(new RequestHeader((short) 18, (short) 4, 1, null)));
        assertContains(keys, 0);
        assertContains(keys, 1);
        assertContains(keys, 18);
        assertContains(keys, 75);
    }

    @Test
    void reportsUnsupportedVersionButStillReturnsApiKeys() {
        byte[] response = ApiVersionsResponse.build(new RequestHeader((short) 18, (short) 9, 1, null));
        ByteBuffer buf = ByteBuffer.wrap(response);

        assertEquals(1, buf.getInt());             // correlation_id echoed
        assertEquals((short) 35, buf.getShort());  // UNSUPPORTED_VERSION
        assertEquals((byte) 5, buf.get());         // still advertises all api keys
    }

    @Test
    void versionZeroIsSupported() {
        byte[] response = ApiVersionsResponse.build(new RequestHeader((short) 18, (short) 0, 1, null));
        assertEquals((short) 0, ByteBuffer.wrap(response).getShort(4)); // error_code at offset 4
    }

    private static void putEntry(ByteBuffer buf, int key, int min, int max) {
        buf.putShort((short) key);
        buf.putShort((short) min);
        buf.putShort((short) max);
        buf.put((byte) 0); // entry tag buffer
    }

    private static int[] apiKeys(byte[] response) {
        ByteBuffer buf = ByteBuffer.wrap(response);
        buf.position(6); // correlation_id + error_code
        int count = (buf.get() & 0xFF) - 1;
        int[] keys = new int[count];
        for (int i = 0; i < count; i++) {
            keys[i] = buf.getShort();
            buf.position(buf.position() + 5); // min, max, tag buffer
        }
        return keys;
    }

    private static void assertContains(int[] values, int wanted) {
        for (int value : values) {
            if (value == wanted) {
                return;
            }
        }
        throw new AssertionError("expected api key " + wanted + " in " + java.util.Arrays.toString(values));
    }
}
