import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

/** Loads binary test fixtures from the test classpath. */
final class Fixtures {

    private Fixtures() {}

    /** A real KRaft {@code __cluster_metadata} log holding one topic ("foo") with one partition. */
    static ByteBuffer clusterMetadataLog() {
        return ByteBuffer.wrap(read("/cluster_metadata_sample.log"));
    }

    private static byte[] read(String resource) {
        try (InputStream in = Fixtures.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("missing test fixture: " + resource);
            }
            return in.readAllBytes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
