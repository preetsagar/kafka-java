import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.junit.jupiter.api.Test;

class ClusterMetadataTest {

    @Test
    void parsesTopicAndPartitionRecordsFromRealLog() {
        ClusterMetadata metadata = ClusterMetadata.parse(Fixtures.clusterMetadataLog());

        ClusterMetadata.Topic foo = metadata.topic("foo");
        assertNotNull(foo);
        assertEquals("bfd99e5e-3235-4552-81f8-d4af1741970c", uuidString(foo.uuid()));

        assertEquals(1, foo.partitions().size());
        ClusterMetadata.Partition partition = foo.partitions().getFirst();
        assertEquals(0, partition.id());
        assertEquals(1, partition.leaderId());
        assertEquals(List.of(1), partition.replicas());
        assertEquals(List.of(1), partition.isr());
    }

    @Test
    void unknownTopicIsNull() {
        assertNull(ClusterMetadata.parse(Fixtures.clusterMetadataLog()).topic("does-not-exist"));
    }

    @Test
    void missingLogFileYieldsEmptyMetadata() {
        assertNull(ClusterMetadata.load(java.nio.file.Path.of("/no/such/metadata.log")).topic("foo"));
    }

    private static String uuidString(byte[] uuid) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < uuid.length; i++) {
            if (i == 4 || i == 6 || i == 8 || i == 10) {
                sb.append('-');
            }
            sb.append(String.format("%02x", uuid[i]));
        }
        return sb.toString();
    }
}
