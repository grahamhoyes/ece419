package app_kvClient.cli;

import app_kvClient.KVClient;


public class DisconnectCommand extends AbstractCommand {

    private final static String commandName = "disconnect";
    private final static String commandDescription = "" +
            "\tTries to disconnect from the connected server.";
    private final static String commandParameters = "";
    private final static String commandOutput = "" +
            "\t\tstatus report: Once the client got disconnected from the server, it should provide a suitable notification to the user.";
    protected final static int expectedArgNum = 0;

    public DisconnectCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput, expectedArgNum);
    }

    @Override
    public void run(KVClient client, String[] tokens) throws Exception {
        try {
            super.run(client, tokens);
            client.closeConnection();
            System.out.println("Disconnected");
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
    }
}
