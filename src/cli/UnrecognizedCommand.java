package cli;

import app_kvClient.KVClient;
import app_kvECS.ECSClient;
import cli.cli_ecsClient.HelpECSClientCommand;
import cli.cli_kvClient.HelpKVClientCommand;

public class UnrecognizedCommand extends AbstractCommand {

    private final static String commandName = "<anything else>";
    private final static String commandDescription = "" +
            "\tAny unrecognized input in the context of this application.";
    private final static String commandParameters = "" +
            "\t\t<any>";
    private final static String commandOutput = "" +
            "\t\terror message: Unknown command";
    protected final static int expectedArgNum = 0;


    public UnrecognizedCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput, expectedArgNum);
    }

    @Override
    public void run(Object client, String[] tokens) throws Exception {
        if (client instanceof KVClient) {
            HelpKVClientCommand.printHelp();
        }
        else if (client instanceof ECSClient) {
            HelpECSClientCommand.printHelp();
        }
        throw new Exception("Unknown command");
    }
}
