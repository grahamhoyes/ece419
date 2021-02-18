package cli.cli_kvClient;

import app_kvClient.KVClient;
import cli.AbstractCommand;


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
    public void run(Object client, String[] tokens) throws Exception {
        super.run(client, tokens);
        ((KVClient) client).closeConnection();
        System.out.println("Disconnected");
    }
}
