package app_kvClient;

import app_kvClient.cli.*;
import client.KVCommInterface;
import client.KVStoreConnection;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class KVClient implements IKVClient, Runnable {

    private static final Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "> ";

    private KVCommInterface storeConnection;
    private boolean running;

    public static void main(String[] args) {
        try {
            new LogSetup("logs/client.log", Level.ALL);
            KVClient client = new KVClient();
            new Thread(client).start();

        } catch (IOException e) {
            System.err.println("Error! Unable to initialize logger.");
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public boolean isRunning() {
        return running;
    }

    @Override
    public void newConnection(String hostname, int port) throws Exception {
        if (this.storeConnection != null) {
            throw new Exception("Connection already established");
        }

        this.storeConnection = new KVStoreConnection(hostname, port);
        this.storeConnection.connect();
    }

    @Override
    public void closeConnection() throws Exception {
        if (this.storeConnection == null) {
            throw new Exception("No connection exists");
        }
        this.storeConnection.disconnect();
        this.storeConnection = null;
    }


    @Override
    public KVCommInterface getStore() {
        return this.storeConnection;
    }

    private void handleCommand(String cmdLine) {
        // TODO: Command handling. This is where all the magic happens.
        //  Use this.storeConnection.connect/disconnect/get/put to interact
        //  with the server

        AbstractCommand command;

        // If there are spaces in the value, they'll need to be joined back
        // together
        String[] tokens = cmdLine.split("\\s+");

        if (tokens.length == 0) return;

        command = switch (tokens[0]) {
            case "connect" -> new ConnectCommand();
            case "disconnect" -> new DisconnectCommand();
            case "put" -> new PutCommand();
            case "get" -> new GetCommand();
            case "logLevel" -> new LogLevelCommand();
            case "help" -> new HelpCommand();
            case "quit" -> new QuitCommand();
            default -> new UnrecognizedCommand();
        };

        command.run(this, tokens);
    }

    public void printError(String message) {
        System.err.println(message);
    }

    @Override
    public void run() {
        this.running = true;
        BufferedReader stdin;

        while (isRunning()) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                handleCommand(cmdLine);
            } catch (IOException e) {
                running = false;
                logger.error("CLI terminated");
            }
        }
    }

}
