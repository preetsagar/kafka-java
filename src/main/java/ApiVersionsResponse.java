import java.nio.ByteBuffer;
import java.util.List;

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

    /** One entry of the {@code api_keys} array: the version range this broker supports for an API. */
    record SupportedApi(short apiKey, short minVersion, short maxVersion) {}

    /** APIs advertised to clients, in wire order. */
    static final List<SupportedApi> SUPPORTED_APIS = List.of(
            new SupportedApi((short) 1, (short) 0, (short) 16),  // Fetch
            new SupportedApi((short) 18, (short) 0, (short) 4),  // ApiVersions
            new SupportedApi((short) 75, (short) 0, (short) 0)); // DescribeTopicPartitions

    private static final int ENTRY_BYTES = 2 + 2 + 2 + 1; // api_key, min, max, tag buffer

    private ApiVersionsResponse() {}

    static byte[] build(RequestHeader request) {
        short errorCode = isSupportedVersion(request.apiVersion()) ? ERROR_NONE : ERROR_UNSUPPORTED_VERSION;

        int size = 4 + 2 + 1 + SUPPORTED_APIS.size() * ENTRY_BYTES + 4 + 1;
        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.putInt(request.correlationId()); // response header v0
        buf.putShort(errorCode);

        buf.put((byte) (SUPPORTED_APIS.size() + 1)); // COMPACT_ARRAY length is N+1
        for (SupportedApi api : SUPPORTED_APIS) {
            buf.putShort(api.apiKey());
            buf.putShort(api.minVersion());
            buf.putShort(api.maxVersion());
            buf.put((byte) 0); // entry tag buffer
        }

        buf.putInt(0);     // throttle_time_ms
        buf.put((byte) 0); // response tag buffer
        return buf.array();
    }

    private static boolean isSupportedVersion(short version) {
        return version >= MIN_VERSION && version <= MAX_VERSION;
    }
}
