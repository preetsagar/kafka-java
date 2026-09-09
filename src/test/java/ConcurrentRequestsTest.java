import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Boots the real broker on port 9092 and exercises it from several connections at once. */
class ConcurrentRequestsTest {

    @BeforeAll
    static void startBroker() throws Exception {
        Thread server = new Thread(() -> Main.main(new String[0]), "broker");
        server.setDaemon(true);
        server.start();
        waitUntilAccepting();
    }

    @Test
    void answersConcurrentClientsWithTheirOwnCorrelationId() throws Exception {
        int clients = 8;
        CyclicBarrier startTogether = new CyclicBarrier(clients);
        List<Thread> threads = new ArrayList<>();
        int[] seen = new int[clients];

        for (int i = 0; i < clients; i++) {
            int correlationId = 1000 + i;
            int slot = i;
            Thread t = new Thread(() -> {
                try (Socket socket = new Socket("localhost", 9092)) {
                    startTogether.await();
                    seen[slot] = roundTrip(socket, correlationId);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            threads.add(t);
            t.start();
        }
        for (Thread t : threads) {
            t.join();
        }

        for (int i = 0; i < clients; i++) {
            assertEquals(1000 + i, seen[i]);
        }
    }

    @Test
    void keepsOneConnectionOpenAcrossMultipleRequests() throws Exception {
        try (Socket socket = new Socket("localhost", 9092)) {
            assertEquals(42, roundTrip(socket, 42));
            assertEquals(43, roundTrip(socket, 43));
            assertEquals(44, roundTrip(socket, 44));
        }
    }

    private static int roundTrip(Socket socket, int correlationId) throws IOException {
        byte[] body = ByteBuffer.allocate(11)
                .putShort((short) 18)   // api_key: ApiVersions
                .putShort((short) 4)    // api_version
                .putInt(correlationId)
                .putShort((short) -1)   // client_id: null
                .put((byte) 0)          // header TAG_BUFFER
                .array();

        OutputStream out = socket.getOutputStream();
        out.write(ByteBuffer.allocate(4 + body.length).putInt(body.length).put(body).array());
        out.flush();

        DataInputStream in = new DataInputStream(socket.getInputStream());
        byte[] response = new byte[in.readInt()];
        in.readFully(response);
        return ByteBuffer.wrap(response).getInt();
    }

    private static void waitUntilAccepting() throws InterruptedException {
        for (int attempt = 0; attempt < 50; attempt++) {
            try (Socket probe = new Socket("localhost", 9092)) {
                return;
            } catch (IOException notYet) {
                Thread.sleep(100);
            }
        }
        throw new IllegalStateException("broker did not start on port 9092");
    }
}
