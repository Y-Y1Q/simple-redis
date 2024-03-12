import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;

public class RedisMain {

    public static void main(String[] args) throws Exception {
        var scheduler = Executors.newVirtualThreadPerTaskExecutor();
        var socket = ServerSocketChannel.open();
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
    private final ConcurrentHashMap<ByteBuf, byte[]> state = new ConcurrentHashMap<>();

    private static final byte[] NOT_FOUND = "$-1\r\n".getBytes(UTF_8);
    private static final byte[] DOLLAR = "$".getBytes(UTF_8);
    private static final byte[] NEW_LINE = "\r\n".getBytes(UTF_8);

    // Parse int, base 10, assuming ASCII, non-negative number
    private int parseInt(ByteBuf toRead){
        var num = 0;
        while(toRead.readableBytes() > 0){
            var b = toRead.readByte();
            var n = b - 48;
            num  = num*10 + n;
        }
        return num;
    }

    public void handleConnection(SocketChannel socket) throws Exception {
        // Configure the channel to be non-blocking: Our Writer and Reader class will control the blocking
        socket.configureBlocking(false);
        // Replace the JDK IO streams with our Reader and Writer
        var writer = new Writer(socket);
        var reader = new Reader(socket, writer);

        while (true) {
            var line = reader.readLine();
            if (line == null)
                break;

            if (line.readByte() != '*')
                throw new RuntimeException("Cannot understand arg batch: " + line);

            byte[] reply = parseAndHandleCommand(reader, line);

            writer.ensureAvailableWriteSpace();
            if (reply == null) {
                writer.writeBytes(NOT_FOUND);
            } else {
                writer.writeBytes(DOLLAR);
                writer.writeLengthAsAscii(reply.length);
                writer.writeBytes(NEW_LINE);
                writer.writeBytes(reply);
                writer.writeBytes(NEW_LINE);
            }
            writer.flushIfBufferedEnough();
        }
    }

    private byte[] parseAndHandleCommand(Reader reader, ByteBuf line) throws Exception {
        // Instead of copying the arguments, parse and interpret the arguments as they are read in.
        // Imagine here a decent 'parser' of the Redis wire protocol grammar
        var argsv = parseInt(line);
        assert (argsv == 2 || argsv == 3);
        var command = readArgument(reader);
        byte[] reply;
        if (command.equals(GET)) {
            assert (argsv == 2);
            var key = readArgument(reader);
            // DANGER-ZONE: We compare mutable objects in a hash map! It violates any recommendation and is super bug prone.
            // However, it does work as the ByteBuf's .hashCode() and .equals() compare the readable bytes,
            // which is the key of the line.
            reply = state.get(key);
        } else if (command.equals(SET)) {
            assert (argsv == 3);
            // DANGER-ZONE: See above. We need a copy this time of the ByteBuf, because it will survive the scope of the parsed line,
            // so it can't be a pointer into the current line.
            var key = readArgument(reader).copy();
            // Content needs to be copied as well, same as above
            var contentLine = readArgument(reader);
            byte[] content = new byte[contentLine.readableBytes()];
            contentLine.readBytes(content);
            reply = state.put(key, content);
        } else {
            throw new IllegalArgumentException("Unknown command: " + command.toString(US_ASCII));
        }
        return reply;
    }

    private static final ByteBuf GET = Unpooled.wrappedBuffer("GET".getBytes(US_ASCII));
    private static final ByteBuf SET = Unpooled.wrappedBuffer("SET".getBytes(US_ASCII));

    private ByteBuf readArgument(Reader reader) throws Exception {
        var line = reader.readLine();
        if (line == null || line.readByte() != '$')
            throw new RuntimeException("Cannot understand arg length: " + line);
        var argLen = parseInt(line);
        line = reader.readLine();
        if (line == null || line.readableBytes() != argLen)
            throw new RuntimeException("Wrong arg length expected " + argLen + " got: " + line);
        return line;
    }
}

class Writer {
    final SocketChannel socket;
    final ByteBuf writeBuffer = Unpooled.buffer(4 * 1024);
    final StringBuilder lengthAsAsciiBuffer = new StringBuilder(2);

    public Writer(SocketChannel socket) throws IOException {
        this.socket = socket;
        assert !socket.isBlocking();
    }

    public void ensureAvailableWriteSpace(){
        ensureAvailableWriteSpace(writeBuffer);
    }

    public void writeBytes(byte[] toWrite) {
        writeBuffer.writeBytes(toWrite);
    }

    public void flushIfBufferedEnough() throws IOException {
        final var AUTO_FLUSH_LIMIT = 1024;
        if (AUTO_FLUSH_LIMIT < writeBuffer.readableBytes()) {
            // A bit confusing in this use case: We read the buffers content into the socket: aka write to the socket
            var written = writeBuffer.readBytes(socket, writeBuffer.readableBytes());
            // If we want proper handling of the back pressure by waiting for the channel to be writable.
            // But for this example we ignore such concerns and just grow the writeBuffer defiantly
        }
    }

    public void writeLengthAsAscii(int length) {
        lengthAsAsciiBuffer.delete(0, lengthAsAsciiBuffer.length());
        lengthAsAsciiBuffer.append(length);
        writeBuffer.writeCharSequence(lengthAsAsciiBuffer, US_ASCII);
    }

    public void flush() throws IOException {
        if (writeBuffer.readableBytes() > 0) {
            writeBuffer.readBytes(socket, writeBuffer.readableBytes());
        }
    }

    // The Netty ByteBufs are not circular buffer: Writes always go to the end and may grow the buffer
    // I assume the underlying reason is to make it more efficient to interact with Java NIO.
    // So, if we're running out of writeable space, discard the bytes already written and
    // copy the not yet read bytes to the start of the buffer, giving it enough space to write more at the end.
    static int ensureAvailableWriteSpace(ByteBuf buf) {
        final var MIN_WRITE_SPACE = 1024;

        if (buf.writableBytes() < MIN_WRITE_SPACE) {
            buf.discardReadBytes();
        }
        return Math.max(MIN_WRITE_SPACE, buf.writableBytes());
    }
}

class Reader {
    final SocketChannel socket;
    final Writer writer;
    final ByteBuf readBuffer = Unpooled.buffer(8 * 1024);
    private final Selector selector;

    private final ByteBuf line = Unpooled.buffer(512);


    public Reader(SocketChannel socket, Writer writer) throws IOException {
        this.socket = socket;
        this.writer = writer;
        this.selector = Selector.open();
        socket.register(selector, SelectionKey.OP_READ, this);
    }

    public ByteBuf readLine() throws Exception {
        var eof = false;
        while (!eof) {
            var readIndex = readBuffer.readerIndex();
            var toIndex = readBuffer.readableBytes();
            // Find the next line in the read content
            var foundNewLine = readBuffer.indexOf(readIndex, readIndex + toIndex, (byte) '\n');
            if (foundNewLine >= 0) {
                var LINE_FEED_SIZE = 1;
                var length = foundNewLine - readIndex;
                // Reset line buffer, then 'copy' the bytes over.
                // We trade avoiding allocating with copying (not that many) bytes
                line.clear();
                line.ensureWritable(length-LINE_FEED_SIZE);
                readBuffer.readBytes(line,length-LINE_FEED_SIZE);
                readBuffer.readerIndex(readIndex + length + LINE_FEED_SIZE);
                return line;
            } else {
                // Otherwise, read from the socket
                int readSize = ensureAvailableWriteSpace(readBuffer);
                // A bit confusing in this use case: We write the content of the socket into the buffer: aka read from the channel
                var read = readBuffer.writeBytes(socket, readSize);
                if (read < 0) {
                    eof = true;
                } else if (read == 0) {
                    // If we read nothing, ensure we flushed our previous reponses
                    writer.flush();
                    // And then wait until the socket becomes readable again
                    selector.select(key -> {
                        if (!key.isReadable()) {
                            throw new AssertionError("Expect to be readable again");
                        }
                    });
                }
            }
        }
        return null;
    }
}