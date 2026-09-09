import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Path;

public class Main {

    private static final int PORT = 9092;
    private static final Path LOG_DIR = Path.of("/tmp/kraft-combined-logs");

    public static void main(String[] args) {
        System.err.println("Logs from your program will appear here!");

        Broker broker = new Broker(
                ClusterMetadata.load(ClusterMetadata.DEFAULT_LOG_PATH),
                new FetchResponse(LOG_DIR),
                new ProduceResponse(LOG_DIR));

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            // The tester restarts the program often; SO_REUSEADDR avoids 'Address already in use'.
            serverSocket.setReuseAddress(true);
            while (true) {
                Socket client = serverSocket.accept();
                Thread.ofVirtual().start(() -> serve(client, broker));
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    private record Broker(ClusterMetadata metadata, FetchResponse fetchResponse, ProduceResponse produceResponse) {}

    private static final short PRODUCE = 0;
    private static final short FETCH = 1;
    private static final short DESCRIBE_TOPIC_PARTITIONS = 75;

    /** Serves requests on a single connection until the client disconnects. */
    private static void serve(Socket client, Broker broker) {
        try (client) {
            DataInputStream in = new DataInputStream(client.getInputStream());
            OutputStream out = client.getOutputStream();

            while (true) {
                byte[] message;
                try {
                    message = new byte[in.readInt()];
                } catch (EOFException disconnected) {
                    return;
                }
                in.readFully(message);

                byte[] response = handle(message, broker);
                out.write(ByteBuffer.allocate(4 + response.length).putInt(response.length).put(response).array());
                out.flush();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    /** Parses one request frame and produces the response bytes (everything after message_size). */
    private static byte[] handle(byte[] message, Broker broker) {
        ByteBuffer buf = ByteBuffer.wrap(message);
        RequestHeader header = RequestHeader.parse(buf);
        return switch (header.apiKey()) {
            case PRODUCE -> broker.produceResponse().build(header, ProduceRequest.parse(buf), broker.metadata());
            case FETCH -> broker.fetchResponse().build(header, FetchRequest.parse(buf), broker.metadata());
            case DESCRIBE_TOPIC_PARTITIONS ->
                    DescribeTopicPartitionsResponse.build(header, DescribeTopicPartitionsRequest.parse(buf), broker.metadata());
            default -> ApiVersionsResponse.build(header);
        };
    }
}
