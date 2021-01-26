package app_kvClient.cli;

import app_kvClient.KVClient;


public class DisconnectCommand extends AbstractCommand {

    private final static String commandName = "disconnect";
    private final static String commandDescription = "" +
            "\tTries to disconnect from the connected server.";
    private final static String commandParameters = "";
    private final static String commandOutput = "" +
            "\t\tstatus report: Once the client got disconnected from the server, it should provide a suitable notification to the user.";

    public DisconnectCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput);
    }

    @Override
    public void run(KVClient client, String[] tokens) {
        try {
            client.closeConnection();
        } catch (Exception e) {
            client.printError(e.getMessage());
        }
        System.out.println("Disconnected");
    }
}
