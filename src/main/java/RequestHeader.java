import java.nio.ByteBuffer;

/**
 * Kafka request header (v2): {@code api_key(INT16) api_version(INT16) correlation_id(INT32)
 * client_id(NULLABLE_STRING) TAG_BUFFER}.
 *
 * <p>{@link #parse} consumes the whole header, leaving {@code buf} positioned at the request body.
 */
record RequestHeader(short apiKey, short apiVersion, int correlationId, String clientId) {

    static RequestHeader parse(ByteBuffer buf) {
        short apiKey = buf.getShort();
        short apiVersion = buf.getShort();
        int correlationId = buf.getInt();
        String clientId = Protocol.readNullableString(buf);
        Protocol.skipTagBuffer(buf);
        return new RequestHeader(apiKey, apiVersion, correlationId, clientId);
    }
}
