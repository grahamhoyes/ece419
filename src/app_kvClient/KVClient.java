package app_kvClient;

import cli.AbstractCommand;
import cli.UnrecognizedCommand;
import cli.cli_kvClient.*;
import client.KVCommInterface;
import client.KVStoreConnection;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.*;

public class KVClient implements IKVClient, Runnable {

    private static final Logger logger = Logger.getLogger("KVClient");
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

    public void setLogLevel(Level level) {
        logger.setLevel(level);
        Logger.getLogger("KVStoreConnection").setLevel(level);
        Logger.getLogger("Connection").setLevel(level);
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

        try {
            this.storeConnection = new KVStoreConnection(hostname, port);
            this.storeConnection.connect();
        } catch (IOException e) {
            this.storeConnection = null;
            throw e;
        }

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

    private void handleCommand(String cmdLine){

        AbstractCommand command;

        // If there are spaces in the value, they'll need to be joined back
        // together
        String[] tokens = cmdLine.split("\\s+");

        if (tokens.length == 0) return;

        switch (tokens[0]) {
            case "connect":
                command = new ConnectCommand();
                break;
            case "disconnect":
                command = new DisconnectCommand();
                break;
            case "put":
                command = new PutCommand();
                break;
            case "get":
                command = new GetCommand();
                break;
            case "logLevel":
                command = new LogLevelCommand();
                break;
            case "help":
                command = new HelpKVClientCommand();
                break;
            case "q":
            case "exit":
            case "quit":
                command = new QuitCommand();
                break;
            default:
                command = new UnrecognizedCommand();
                break;
        }

        try {
            command.run(this, tokens);
        } catch (Exception e) {
            logger.error("Something went wrong: " + e.getMessage());
            System.out.println(e.getMessage());
            if (e instanceof  IOException) {
                this.storeConnection = null;
            }
        }

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
