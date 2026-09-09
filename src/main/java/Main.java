import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
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
        in.skipBytes(requestSize);

        // Response: message_size (INT32) + header v0 (correlation_id INT32).
        // Correlation ID hardcoded to 7; parsing it from the request is the next stage.
        OutputStream out = clientSocket.getOutputStream();
        out.write(new byte[] {0, 0, 0, 4, 0, 0, 0, 7});
        out.flush();
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }
}
