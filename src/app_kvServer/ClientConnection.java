package app_kvServer;

import shared.Connection;
import shared.messages.DeserializationException;
import shared.messages.JsonKVMessage;

import java.io.IOException;
import java.net.Socket;


/**
 * Represents a connection to a client over a TCP socket. Used
 * by the server.
 */
public class ClientConnection extends Connection implements Runnable {

    private boolean isOpen;
    private final IKVServer server;

    public ClientConnection(Socket socket, IKVServer server) {
        this.socket = socket;
        this.hostname = socket.getInetAddress().getHostName();
        this.port = socket.getPort();
        this.server = server;
        this.isOpen = true;
    }

    public void run() {
        try {
            output = socket.getOutputStream();
            input = socket.getInputStream();

            while (isOpen) {
                try {
                    JsonKVMessage message = receiveMessage();
                    // TODO: Do something with the message
                    sendMessage(message);
                } catch (DeserializationException e) {
                    logger.error(e.getMessage());
                } catch (IOException e) {
                    logger.error(e);
                    isOpen = false;
                }

            }

        } catch (IOException e) {
            logger.error("Error! Connection could not be established");
            isOpen = false;
        } finally {
            disconnect();

            logger.info("Client connection to "
                    + hostname + ":" + port + " has been closed");

        }
    }
}
