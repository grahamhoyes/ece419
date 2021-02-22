package app_kvServer;

import ecs.HashRing;
import shared.Connection;
import shared.messages.DeserializationException;
import shared.messages.JsonKVMessage;
import shared.messages.KVMessage.StatusType;

import java.io.FileNotFoundException;
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

                    if (server.getStatus() == IKVServer.ServerStatus.STOPPED) {
                        res.setStatus(StatusType.SERVER_STOPPED);
                        res.setMessage("Server is stopped");
                        sendMessage(res);
                        continue;
                    }


                    res.setKey(req.getKey());
                    res.setValue(req.getValue());

                    if (!server.isNodeResponsible(req.getKey())) {
                        res.setStatus(StatusType.SERVER_NOT_RESPONSIBLE);
                        res.setMessage("Server is not responsible for the given key");

                        HashRing metadata = server.getMetadata();
                        res.setMetadata(metadata);
                        sendMessage(res);
                        continue;
                    }

                    switch (req.getStatus()) {
                        case GET:
                            try {
                                String value = server.getKV(req.getKey());
                                res.setStatus(StatusType.GET_SUCCESS);
                                res.setValue(value);
                                logger.info("GET " + req.getKey() + " successful: " + res.getValue());
                            } catch (Exception e) {
                                if (e instanceof FileNotFoundException){
                                    throw (FileNotFoundException)e;
                                }
                                res.setStatus(StatusType.GET_ERROR);
                                res.setMessage(e.getMessage());
                                logger.warn("GET " + req.getKey() + " error: " + e.getMessage());
                            }
                            break;
                        case PUT:
                            if (server.getStatus() == IKVServer.ServerStatus.WRITE_LOCKED) {
                                res.setStatus(StatusType.SERVER_WRITE_LOCK);
                                res.setMessage("Server locked for write");
                                logger.warn("Could not process PUT request, server write locked");
                                break;
                            }

                            if (req.getValue() == null || req.getValue().equals("null")) {
                                // The value string "null" is used to trigger deletion,
                                // because the spec is insane

                                try {
                                    server.deleteKV(req.getKey());
                                    res.setStatus(StatusType.DELETE_SUCCESS);
                                    logger.info("DELETE " + req.getKey() + " successful");
                                } catch (Exception e) {
                                    if (e instanceof FileNotFoundException){
                                        throw (FileNotFoundException)e;
                                    }
                                    res.setStatus(StatusType.DELETE_ERROR);
                                    res.setMessage(e.getMessage());
                                    logger.warn("DELETE " + req.getKey() + " error: " + e.getMessage());
                                }
                            } else {
                                try {
                                    boolean exists = server.putKV(req.getKey(), req.getValue());
                                    if (exists) {
                                        res.setStatus(StatusType.PUT_UPDATE);
                                        logger.info("PUT update " + req.getKey() + "="
                                                + req.getValue() + " successful");
                                    } else {
                                        res.setStatus(StatusType.PUT_SUCCESS);
                                        logger.info("PUT insert " + req.getKey() + "="
                                                + req.getValue() + " successful");
                                    }
                                } catch (Exception e) {
                                    if (e instanceof FileNotFoundException){
                                        throw (FileNotFoundException)e;
                                    }
                                    res.setStatus(StatusType.PUT_ERROR);
                                    res.setMessage(e.getMessage());
                                    logger.warn("PUT " + req.getKey() + "="
                                            + req.getValue() + " error: " + e.getMessage());
                                }
                            }
                            break;
                        default:
                            // TODO: Test this
                            res.setStatus(StatusType.BAD_REQUEST);
                            res.setMessage("Invalid status");
                            logger.warn("Invalid request type");
                            break;
                    }

                    sendMessage(res);

                } catch (DeserializationException e) {
                    res.setStatus(StatusType.BAD_REQUEST);
                    res.setMessage(e.getMessage());
                    sendMessage(res);
                    logger.error(e.getMessage());
                } catch (FileNotFoundException e){
                    res.setStatus(StatusType.SERVER_ERROR);
                    res.setMessage("Storage file not found");
                    sendMessage(res);
                    logger.fatal("Error: Storage file not found.");
                    server.kill();
                } catch (IOException e) {
                    // "Connection terminated" is thrown by us when the connection is closed
                    // by the client
                    if (e.getMessage().equals("Connection terminated")) {
                        logger.info("Connection closed by client");
                    } else {
                        // IOException indicates something wrong with the socket,
                        // so the connection is terminated
                        logger.error(e);
                        isOpen = false;
                    }
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
