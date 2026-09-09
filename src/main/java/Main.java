import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {

    private static final int PORT = 9092;

    public static void main(String[] args) {
        System.err.println("Logs from your program will appear here!");

        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            // The tester restarts the program often; SO_REUSEADDR avoids 'Address already in use'.
            serverSocket.setReuseAddress(true);
            while (true) {
                Socket client = serverSocket.accept();
                Thread.ofVirtual().start(() -> serve(client));
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    /** Serves requests on a single connection until the client disconnects. */
    private static void serve(Socket client) {
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

                byte[] response = handle(message);
                out.write(ByteBuffer.allocate(4 + response.length).putInt(response.length).put(response).array());
                out.flush();
            }
        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }

    private static final short DESCRIBE_TOPIC_PARTITIONS = 75;

    /** Parses one request frame and produces the response bytes (everything after message_size). */
    private static byte[] handle(byte[] message) {
        ByteBuffer buf = ByteBuffer.wrap(message);
        RequestHeader header = RequestHeader.parse(buf);
        return switch (header.apiKey()) {
            case DESCRIBE_TOPIC_PARTITIONS ->
                    DescribeTopicPartitionsResponse.build(header, DescribeTopicPartitionsRequest.parse(buf));
            default -> ApiVersionsResponse.build(header);
        };
    }
}
