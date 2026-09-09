import java.nio.ByteBuffer;

/**
 * Builds the {@code ApiVersions} (API key 18) response, version 4.
 *
 * <p>Returned bytes are the response header (v0) plus body, i.e. everything after the
 * leading {@code message_size} field.
 */
final class ApiVersionsResponse {

    static final short API_KEY = 18;
    static final short MIN_VERSION = 0;
    static final short MAX_VERSION = 4;
    static final short ERROR_NONE = 0;
    static final short ERROR_UNSUPPORTED_VERSION = 35;

    private ApiVersionsResponse() {}

    static byte[] build(RequestHeader request) {
        short errorCode = isSupportedVersion(request.apiVersion()) ? ERROR_NONE : ERROR_UNSUPPORTED_VERSION;

        ByteBuffer buf = ByteBuffer.allocate(19);
        buf.putInt(request.correlationId()); // response header v0
        buf.putShort(errorCode);
        buf.put((byte) 2); // api_keys: COMPACT_ARRAY length is N+1, one entry
        buf.putShort(API_KEY);
        buf.putShort(MIN_VERSION);
        buf.putShort(MAX_VERSION);
        buf.put((byte) 0); // entry tag buffer
        buf.putInt(0); // throttle_time_ms
        buf.put((byte) 0); // response tag buffer
        return buf.array();
    }

    private static boolean isSupportedVersion(short version) {
        return version >= MIN_VERSION && version <= MAX_VERSION;
    }
}
