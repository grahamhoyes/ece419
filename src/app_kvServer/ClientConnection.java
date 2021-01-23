package app_kvServer;

import shared.Connection;
import shared.messages.JsonKVMessage;

import java.io.IOException;
import java.net.Socket;


/**
 * Represents a connection to a client over a TCP socket. Used
 * by the server.
 */
public class ClientConnection extends Connection implements Runnable {

    private boolean isOpen;

    public ClientConnection(Socket socket) {
        this.socket = socket;
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
                } catch (IOException e) {
                    logger.error("Error! Connection lost", e);
                    isOpen = false;
                }

            }

        } catch (IOException e) {
            logger.error("Error! Connection could not be established");
            isOpen = false;
        } finally {

            try {
                if (socket != null) {
                    input.close();
                    output.close();
                    socket.close();
                }
            } catch (IOException e) {
                logger.error("Error! Unable to close connection", e);
            }

        }
    }
}
