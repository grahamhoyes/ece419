package cli.cli_ecsClient;

import cli.AbstractCommand;

public class RemoveNodeCommand extends AbstractCommand {

    private final static String commandName = "removeNode <index of server>";
    private final static String commandDescription = "" +
            "\tRemove a server from the storage service at an arbitrary position.";
    private final static String commandParameters = "" +
            "\t\tindex of server: integer, index of the server to remove";
    private final static String commandOutput = "" +
            "\t\tstatus report: Once the node has been removed, it should provide a suitable notification to the user.";
    protected final static int expectedArgNum = 1;

    public RemoveNodeCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput, expectedArgNum);
    }
}
