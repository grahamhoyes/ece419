package cli.cli_ecsClient;

import app_kvECS.ECSClient;
import cli.AbstractCommand;
import ecs.IECSNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class RemoveNodeCommand extends AbstractCommand {

    private final static String commandName = "removeNode <node names, separated by an empty space>";
    private final static String commandDescription = "" +
            "\tRemove servers from the storage service.";
    private final static String commandParameters = "" +
            "\t\tnode names: strings of the node names, separated by an empty space";
    private final static String commandOutput = "" +
            "\t\tstatus report: Once the node has been removed, it should provide a suitable notification to the user.";
    protected final static int expectedArgNum = 1;

    public RemoveNodeCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput, expectedArgNum);
    }

    @Override
    public void run(Object client, String[] tokens) throws Exception {
        if (tokens.length < 2) {
            throw new Exception("Invalid number of arguments. Usage: " + commandName);
        }

        List<String> serverNames = new ArrayList<>(Arrays.asList(tokens));
        serverNames.remove(0);

        boolean result = ((ECSClient) client).removeNodes(serverNames);
        System.out.println("Nodes removed.");
    }
}
