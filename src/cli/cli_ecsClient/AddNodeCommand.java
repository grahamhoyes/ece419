package cli.cli_ecsClient;

import app_kvECS.ECSClient;
import cli.AbstractCommand;
import ecs.IECSNode;
import shared.messages.KVMessage;

public class AddNodeCommand extends AbstractCommand {

    private final static String commandName = "addNode";
    private final static String commandDescription = "" +
            "\tCreate a new KVServer and add it to the storage service at an arbitrary position.";
    private final static String commandParameters = "";
    private final static String commandOutput = "" +
            "\t\tstatus report: Once the server is created, it should provide a suitable notification to the user.";
    protected final static int expectedArgNum = 0;

    public AddNodeCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput, expectedArgNum);
    }

    @Override
    public void run(Object client, String[] tokens) throws Exception {
        super.run(client, tokens);
        IECSNode node = ((ECSClient) client).getECS().addNode();
        System.out.println("Node added.");
    }
}
