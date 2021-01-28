package testing;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import shared.Connection;
import shared.messages.DeserializationException;
import shared.messages.JsonKVMessage;
import shared.messages.KVMessage;

import java.io.*;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Test the communication protocol used by the TCP
 * connections, implemented in the abstract Connection class.
 *
 * Uses Mockito to mock socket behaviours. Input/output streams
 * are mocked using ByteArrayInput/OutputStream, so data can be
 * written to / read from an underlying byte array.
 *
 * Written by Graham Hoyes
 */
public class CommunicationProtocolTest extends Assert {
    Socket mockSocket = Mockito.mock(Socket.class, Mockito.RETURNS_DEEP_STUBS);

    @Before
    public void init() {
        Mockito.when(mockSocket.getInetAddress().getHostName()).thenReturn("localhost");
        Mockito.when(mockSocket.getPort()).thenReturn(8008);
    }

    @Test
    public void testSendMessage() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        SampleConnection conn = new SampleConnection(null, output);

        JsonKVMessage msg = new JsonKVMessage(KVMessage.StatusType.GET_SUCCESS);
        msg.setKey("foo");
        msg.setValue("bar");
        String msgContent = msg.serialize();

        conn.sendMessage(msg);

        ByteBuffer data = ByteBuffer.wrap(output.toByteArray());
        data.order(ByteOrder.LITTLE_ENDIAN);

        // The first byte is the protocol
        byte protocolVersion = data.get();
        assertEquals(protocolVersion, Connection.PROTOCOL_VERSION);

        // The next 4 bytes are the payload size
        int payloadSize = data.getInt();
        assertEquals(payloadSize, msgContent.getBytes(StandardCharsets.UTF_8).length);

        // The remainder of the header should be empty
        for (int i = data.position(); i < Connection.HEADER_SIZE; i++) {
            assertEquals(data.get(), 0);
        }

        // The message
        byte[] sentMessage = new byte[payloadSize];
        data.get(sentMessage, 0, payloadSize);

        assertEquals(new String(sentMessage), msgContent);
    }

    @Test
    public void testReceiveMessage() throws IOException, DeserializationException {
        // Construct a protocol version 1 message
        JsonKVMessage msg = new JsonKVMessage(KVMessage.StatusType.GET_SUCCESS);
        msg.setKey("foo");
        msg.setValue("bar");
        String msgContent = msg.serialize();
        byte[] msgBytes = msgContent.getBytes(StandardCharsets.UTF_8);

        ByteBuffer data = ByteBuffer.allocate(Connection.HEADER_SIZE + msgBytes.length);
        data.order(ByteOrder.LITTLE_ENDIAN);

        data.put(Connection.PROTOCOL_VERSION);
        data.putInt(msgBytes.length);

        for (int i = data.position(); i < Connection.HEADER_SIZE; i++) {
            data.put((byte)0);
        }

        data.put(msgBytes, 0, msgBytes.length);

        ByteArrayInputStream input = new ByteArrayInputStream(data.array());

        SampleConnection conn = new SampleConnection(input, null);
        JsonKVMessage receivedMessage = conn.receiveMessage();

        assertEquals(receivedMessage.getStatus(), msg.getStatus());
        assertEquals(receivedMessage.getKey(), msg.getKey());
        assertEquals(receivedMessage.getValue(), msg.getValue());
        assertEquals(receivedMessage.getMessage(), msg.getMessage());
    }

    @Test(expected = IOException.class)
    public void testThrowsIOExceptionForClosedInputStream() throws IOException, DeserializationException {
        ByteArrayInputStream input = new ByteArrayInputStream(new byte[] {});

        SampleConnection conn = new SampleConnection(input, null);
        input.close();
        JsonKVMessage receivedMessage = conn.receiveMessage();
    }

    private class SampleConnection extends Connection {

        public SampleConnection(InputStream input, OutputStream output) {
            this.hostname = "localhost";
            this.port = 8008;

            this.input = input;
            this.output = output;
            this.socket = mockSocket;
        }
    }
}
