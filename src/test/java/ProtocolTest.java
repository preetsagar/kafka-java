import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ProtocolTest {

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 127, 128, 300, 16_383, 16_384, 2_097_151, Integer.MAX_VALUE})
    void unsignedVarintRoundTrips(int value) {
        ByteBuffer buf = ByteBuffer.allocate(5);
        Protocol.writeUnsignedVarint(buf, value);
        buf.flip();
        assertEquals(value, Protocol.readUnsignedVarint(buf));
    }

    @Test
    void readsCompactString() {
        ByteBuffer buf = ByteBuffer.allocate(16);
        byte[] bytes = "foo".getBytes(UTF_8);
        Protocol.writeUnsignedVarint(buf, bytes.length + 1);
        buf.put(bytes).flip();

        assertEquals("foo", Protocol.readCompactString(buf));
    }

    @Test
    void writesCompactNullableString() {
        ByteBuffer buf = ByteBuffer.allocate(16);
        Protocol.writeCompactNullableString(buf, "bar");
        buf.flip();
        assertEquals("bar", Protocol.readCompactString(buf));
    }

    @Test
    void writesNullAsSingleZeroByte() {
        ByteBuffer buf = ByteBuffer.allocate(4);
        Protocol.writeCompactNullableString(buf, null);
        assertEquals(1, buf.position());
        assertEquals(0, buf.get(0));
    }
}
