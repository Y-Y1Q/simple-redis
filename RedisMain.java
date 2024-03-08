import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

public class RedisMain {

    public static void main(String[] args) throws IOException {
        var scheduler = Executors.newCachedThreadPool();
        var socket = new ServerSocket();
        socket.bind(new InetSocketAddress("0.0.0.0", 16379));

        System.out.println("App is listening on 0.0.0.0:16379");
        var clone = new RedisClone();
        while (true) {
            var client = socket.accept();
            scheduler.execute(() -> {
                try (client) {
                    clone.handleConnection(client);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

    }
}

class RedisClone {
    private final ConcurrentHashMap<String, String> state = new ConcurrentHashMap<>();

    public void handleConnection(Socket socket) throws Exception {
        var args = new ArrayList<String>();
        var reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        var writer = new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8);
        while (true) {
            args.clear();
            var line = reader.readLine();
            if (line == null)
                break;

            if (line.charAt(0) != '*')
                throw new RuntimeException("Cannot understand arg batch: " + line);

            var argsv = Integer.parseInt(line.substring(1));
            for (int i = 0; i < argsv; i++) {
                line = reader.readLine();
                if (line == null || line.charAt(0) != '$')
                    throw new RuntimeException("Cannot understand arg length: " + line);
                var argLen = Integer.parseInt(line.substring(1));
                line = reader.readLine();
                if (line == null || line.length() != argLen)
                    throw new RuntimeException("Wrong arg length expected " + argLen + " got: " + line);

                args.add(line);
            }

            var reply = executeCommand(args);
            if (reply == null) {
                writer.write("$-1\r\n");
            } else {
                writer.write("$" + reply.length() + "\r\n" + reply + "\r\n");
            }
            writer.flush();
        }
    }

    String executeCommand(List<String> args) {
        switch (args.get(0)) {
            case "GET":
                return state.get(args.get(1));
            case "SET":
                state.put(args.get(1), args.get(2));
                return null;
            default:
                throw new IllegalArgumentException("Unknown command: " + args.get(1));
        }
    }
}