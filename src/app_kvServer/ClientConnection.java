package app_kvServer;

import shared.Connection;
import shared.messages.DeserializationException;
import shared.messages.JsonKVMessage;
import shared.messages.KVMessage.StatusType;

import java.io.IOException;
import java.net.Socket;


/**
 * Represents a connection to a client over a TCP socket. Used
 * by the server.
 */
public class ClientConnection extends Connection implements Runnable {

    private final IKVServer server;
    private boolean isOpen;

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
                JsonKVMessage res = new JsonKVMessage();

                try {
                    JsonKVMessage req = receiveMessage();

                    switch (req.getStatus()) {
                        case GET:
                            try {
                                String value = server.getKV(req.getKey());
                                res.setStatus(StatusType.GET_SUCCESS);
                                res.setKey(req.getKey());
                                res.setValue(value);
                            } catch (Exception e) {
                                res.setStatus(StatusType.GET_ERROR);
                                res.setKey(req.getKey());
                                res.setMessage(e.getMessage());
                            }
                            break;
                        case PUT:
                            if (res.getValue().equals("null")) {
                                // The value string "null" is used to trigger deletion,
                                // because the spec is insane

                                try {
                                    server.deleteKV(req.getKey());
                                    res.setStatus(StatusType.DELETE_SUCCESS);
                                } catch (Exception e) {
                                    res.setStatus(StatusType.DELETE_ERROR);
                                    res.setKey(req.getKey());
                                    res.setMessage(e.getMessage());
                                }
                            } else {
                                try {
                                    boolean exists = server.putKV(req.getKey(), req.getValue());
                                    if (exists)
                                        res.setStatus(StatusType.PUT_SUCCESS);
                                    else
                                        res.setStatus(StatusType.PUT_UPDATE);
                                    res.setKey(req.getKey());
                                } catch (Exception e) {
                                    res.setStatus(StatusType.PUT_ERROR);
                                    res.setKey(req.getKey());
                                    res.setMessage(e.getMessage());
                                }
                            }
                            break;
                        default:
                            res.setStatus(StatusType.BAD_REQUEST);
                            res.setMessage("Invalid status");
                            break;
                    }

                    sendMessage(res);

                } catch (DeserializationException e) {
                    res.setStatus(StatusType.BAD_REQUEST);
                    res.setMessage(e.getMessage());
                    sendMessage(res);
                    logger.error(e.getMessage());
                } catch (IOException e) {
                    // IOException indicates something wrong with the socket,
                    // so the connection is terminated
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
