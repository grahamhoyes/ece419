package shared;

import org.apache.log4j.Logger;
import shared.messages.DeserializationException;
import shared.messages.JsonKVMessage;
import shared.messages.KVMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/*
 * Abstract connection class with methods that are common to both
 * the client and server for communications purpose.
 */
public abstract class Connection {
    protected static final Logger logger = Logger.getRootLogger();

    // Header size in bytes
    private static final int HEADER_SIZE = 16;
    private static final byte PROTOCOL_VERSION = 1;

    protected String hostname;
    protected int port;

    protected Socket socket;
    protected InputStream input;
    protected OutputStream output;

    /**
     * Send a message using this connection's socket.
     * <p>
     * Serialized message strings are decoded and sent as bytes. The packet
     * begins with a 16-byte header. The first byte indicates the protocol
     * version (here, 1). The next 4 bytes indicate the size of the message
     * body (little endian). The remaining 11 bytes are currently unused.
     *
     * @param message The message to send
     * @throws IOException Exceptions to do with the output buffer
     */
    public void sendMessage(KVMessage message) throws IOException {
        String msgContent = message.serialize();
        byte[] rawMsgBytes = msgContent.getBytes();

        ByteBuffer msgBuffer = ByteBuffer.allocate(rawMsgBytes.length + HEADER_SIZE);
        msgBuffer.order(ByteOrder.LITTLE_ENDIAN);

        // Add the 16-byte header to the beginning of the message
        msgBuffer.put(PROTOCOL_VERSION);
        msgBuffer.putInt(rawMsgBytes.length);

        for (int i = msgBuffer.position(); i < HEADER_SIZE; i++) {
            msgBuffer.put((byte) 0);
        }

        // Add the rest of the message
        msgBuffer.put(rawMsgBytes, 0, rawMsgBytes.length);

        byte[] msgBytes = msgBuffer.array();

        // Send the message
        output.write(msgBytes, 0, msgBytes.length);
        output.flush();

        logger.info("SEND \t"
                + socket.getInetAddress().getHostName() + ":"
                + socket.getPort() + ">: '"
                + msgContent + "'");
    }


    /**
     * Receive a message from this connection's socket.
     * <p>
     * Follows the same semantics as sendMessage.
     *
     * @return JsonKVMessage of the message
     * @throws IOException              Exception to do with the input buffer
     * @throws DeserializationException For issues decoding the message payload
     */
    public JsonKVMessage receiveMessage() throws IOException, DeserializationException {
        byte[] headerBytes = new byte[16];

        int headerLen = input.read(headerBytes, 0, HEADER_SIZE);

        if (headerLen == -1) {
            throw new IOException("Connection terminated by client");
        } else if (headerLen != HEADER_SIZE) {
            throw new IOException("Failed to read message header");
        }

        ByteBuffer headerBuffer = ByteBuffer.wrap(headerBytes);
        headerBuffer.order(ByteOrder.LITTLE_ENDIAN);
        byte protocolVersion = headerBuffer.get();

        if (protocolVersion != PROTOCOL_VERSION) {
            logger.warn("Received an invalid protocol version " + protocolVersion
                    + ", behaviour may be undefined");
        }

        int messageLength = headerBuffer.getInt();

        byte[] msgBytes = new byte[messageLength];

        if (input.read(msgBytes, 0, messageLength) != messageLength) {
            throw new IOException("Failed to read message body");
        }

        String msg = new String(msgBytes);

        JsonKVMessage message = new JsonKVMessage(msg);

        logger.info("RECEIVE \t<"
                + socket.getInetAddress().getHostAddress() + ":"
                + socket.getPort() + ">: "
                + msg + "'");

        return message;
    }

}
