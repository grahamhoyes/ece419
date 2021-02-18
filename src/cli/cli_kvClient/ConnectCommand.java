package cli.cli_kvClient;

import app_kvClient.KVClient;
import cli.AbstractCommand;

public class ConnectCommand extends AbstractCommand {

    protected final static String commandName = "connect <address> <port>";
    protected final static String commandDescription = "" +
            "\tTries to establish a TCP-connection to the storage server based on the given server address and the port number of the storage service.";
    protected final static String commandParameters = "" +
            "\t\taddress: Hostname or IP address of the storage server.\n" +
            "\t\tport: The port of the storage service on the respective server.";
    protected final static String commandOutput = "" +
            "\t\tserver reply: Once the connection is established, the client program should give a status message to the user.";
    protected final static int expectedArgNum = 2;

    public ConnectCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput, expectedArgNum);
    }

    @Override
    public void run(Object client, String[] tokens) throws Exception {
        try {
            super.run(client, tokens);

            String hostname = tokens[1];
            int port = Integer.parseInt(tokens[2]);
            ((KVClient) client).newConnection(hostname, port);
            System.out.println("Connection established");
        } catch (NumberFormatException e) {
            throw new Exception("Not a valid address. Port must be an integer");
        }
    }
}
