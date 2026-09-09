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
        int correlationId = ByteBuffer.wrap(request, 4, 4).getInt();

        // Response: message_size (INT32) + header v0 (correlation_id INT32).
        OutputStream out = clientSocket.getOutputStream();
        out.write(ByteBuffer.allocate(8).putInt(4).putInt(correlationId).array());
        out.flush();
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }
}
