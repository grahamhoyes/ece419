package app_kvClient.cli;

import app_kvClient.KVClient;

public class ConnectCommand extends AbstractCommand {

    protected final static String commandName = "connect <address> <port>";
    protected final static String commandDescription = "" +
            "\tTries to establish a TCP-connection to the storage server based on the given server address and the port number of the storage service.";
    protected final static String commandParameters = "" +
            "\t\taddress: Hostname or IP address of the storage server.\n" +
            "\t\tport: The port of the storage service on the respective server.";
    protected final static String commandOutput = "" +
            "\t\tserver reply: Once the connection is established, the client program should give a status message to the user.";

    public ConnectCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput);
    }

    @Override
    public void run(KVClient client, String[] tokens) {
        try {
            if (tokens.length != 3) {
                client.printError("Invalid number of arguments. Usage: connect <address> <port>");
                return;
            }

            String hostname = tokens[1];
            int port = Integer.parseInt(tokens[2]);
            client.newConnection(hostname, port);
        } catch (NumberFormatException e) {
            client.printError("Not a valid address. Port must be an integer");
        } catch (Exception e) {
            client.printError(e.getMessage());
        }

        System.out.println("Connection established");
    }
}
