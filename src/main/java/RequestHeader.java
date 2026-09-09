import java.nio.ByteBuffer;

/**
 * Kafka request header (v2), limited to the fields this broker currently needs.
 *
 * <p>Wire layout: {@code api_key(INT16) api_version(INT16) correlation_id(INT32) ...}
 */
record RequestHeader(short apiKey, short apiVersion, int correlationId) {

    /** Parses the header from a message body that excludes the leading {@code message_size}. */
    static RequestHeader parse(byte[] message) {
        if (message.length < 8) {
            throw new IllegalArgumentException("request header needs 8 bytes, got " + message.length);
        }
        ByteBuffer buf = ByteBuffer.wrap(message);
        return new RequestHeader(buf.getShort(), buf.getShort(), buf.getInt());
    }
}
