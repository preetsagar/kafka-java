import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;

/** Read/write helpers for Kafka wire protocol primitives (KIP-482 compact types). */
final class Protocol {

    private Protocol() {}

    static int readUnsignedVarint(ByteBuffer buf) {
        int value = 0;
        int shift = 0;
        int b;
        do {
            b = buf.get() & 0xFF;
            value |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return value;
    }

    static void writeUnsignedVarint(ByteBuffer buf, int value) {
        while ((value & ~0x7F) != 0) {
            buf.put((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        buf.put((byte) value);
    }

    /** Signed VARINT (zig-zag): used inside record batches (record size, deltas, key/value lengths). */
    static long readSignedVarint(ByteBuffer buf) {
        long raw = readUnsignedVarint(buf) & 0xFFFFFFFFL;
        return (raw >>> 1) ^ -(raw & 1);
    }

    /** COMPACT_ARRAY / COMPACT_BYTES length: UNSIGNED_VARINT of {@code count + 1}, {@code 0} meaning null. */
    static int readCompactLength(ByteBuffer buf) {
        return readUnsignedVarint(buf) - 1;
    }

    /** NULLABLE_STRING: INT16 length prefix, {@code -1} meaning null. */
    static String readNullableString(ByteBuffer buf) {
        short length = buf.getShort();
        if (length < 0) {
            return null;
        }
        byte[] bytes = new byte[length];
        buf.get(bytes);
        return new String(bytes, UTF_8);
    }

    /** COMPACT_STRING: UNSIGNED_VARINT of {@code length + 1}, then UTF-8 bytes. */
    static String readCompactString(ByteBuffer buf) {
        int length = readUnsignedVarint(buf) - 1;
        byte[] bytes = new byte[length];
        buf.get(bytes);
        return new String(bytes, UTF_8);
    }

    /** COMPACT_NULLABLE_STRING: varint {@code 0} for null, otherwise {@code length + 1} then bytes. */
    static void writeCompactNullableString(ByteBuffer buf, String value) {
        if (value == null) {
            writeUnsignedVarint(buf, 0);
            return;
        }
        byte[] bytes = value.getBytes(UTF_8);
        writeUnsignedVarint(buf, bytes.length + 1);
        buf.put(bytes);
    }

    /** Skips a TAG_BUFFER; the tester sends none, but skip properly if it ever does. */
    static void skipTagBuffer(ByteBuffer buf) {
        int taggedFields = readUnsignedVarint(buf);
        for (int i = 0; i < taggedFields; i++) {
            readUnsignedVarint(buf); // tag
            int size = readUnsignedVarint(buf);
            buf.position(buf.position() + size);
        }
    }

    static void writeEmptyTagBuffer(ByteBuffer buf) {
        buf.put((byte) 0);
    }
}
