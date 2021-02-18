package cli.cli_ecsClient;

import app_kvECS.ECSClient;
import cli.AbstractCommand;
import ecs.IECSNode;

import java.util.Collection;

public class AddNumberOfNodesCommand extends AbstractCommand {

    private final static String commandName = "addNodes <numberOfNodes>";
    private final static String commandDescription = "" +
            "\tRandomly choose <numberOfNodes> servers from the available machines and start the KVServer by issuing an SSH call to the respective machine." +
            "\tThis call launches the storage server. For simplicity, locate the KVServer.jar in the same directory as the ECS." +
            "\tAll storage servers are initialized with the metadata and any persisted data, and remain in state stopped.";
    private final static String commandParameters = "" +
            "\t\tnumberOfNodes: integer, number of nodes to add";
    private final static String commandOutput = "" +
            "\t\tstatus report: Once the nodes are added, it should provide a suitable notification to the user.";
    protected final static int expectedArgNum = 1;

    public AddNumberOfNodesCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput, expectedArgNum);
    }

    @Override
    public void run(Object client, String[] tokens) throws Exception {
        super.run(client, tokens);
        int numberOfNodes = Integer.parseInt(tokens[1]);
        Collection<IECSNode> nodes = ((ECSClient) client).addNodes(numberOfNodes);
        System.out.println("Nodes added.");
    }
}
