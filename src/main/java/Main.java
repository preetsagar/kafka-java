import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.net.Socket;

public class Main {
  public static void main(String[] args) {
    System.err.println("Logs from your program will appear here!");

    int port = 9092;
    try (ServerSocket serverSocket = new ServerSocket(port)) {
      serverSocket.setReuseAddress(true);
      try (Socket clientSocket = serverSocket.accept()) {
        DataInputStream in = new DataInputStream(clientSocket.getInputStream());
        int requestSize = in.readInt();
        byte[] request = new byte[requestSize];
        in.readFully(request);

        // Request header v2: api_key(2) + api_version(2) + correlation_id(4) + ...
        short apiVersion = ByteBuffer.wrap(request, 2, 2).getShort();
        int correlationId = ByteBuffer.wrap(request, 4, 4).getInt();
        short errorCode = (apiVersion >= 0 && apiVersion <= 4) ? 0 : (short) 35; // UNSUPPORTED_VERSION

        // ApiVersions v4 response body: error_code, api_keys COMPACT_ARRAY, throttle_time_ms, tag_buffer.
        ByteBuffer body = ByteBuffer.allocate(64);
        body.putShort(errorCode);
        body.put((byte) 2); // COMPACT_ARRAY length = N+1, one entry
        body.putShort((short) 18); // API_VERSIONS
        body.putShort((short) 0);  // min_version
        body.putShort((short) 4);  // max_version
        body.put((byte) 0);        // entry tag_buffer
        body.putInt(0);            // throttle_time_ms
        body.put((byte) 0);        // response tag_buffer
        body.flip();

        // message_size + response header v0 (correlation_id) + body.
        ByteBuffer resp = ByteBuffer.allocate(4 + 4 + body.remaining());
        resp.putInt(4 + body.remaining());
        resp.putInt(correlationId);
        resp.put(body);

        OutputStream out = clientSocket.getOutputStream();
        out.write(resp.array());
        out.flush();
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }
}
