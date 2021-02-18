package cli.cli_ecsClient;

import app_kvECS.ECSClient;
import cli.AbstractCommand;

public class StopCommand extends AbstractCommand {

    private final static String commandName = "stop";
    private final static String commandDescription = "" +
            "\tStops the service; all participating KVServers are stopped for processing client requests but the processes remain running.";
    private final static String commandParameters = "";
    private final static String commandOutput = "" +
            "\t\tstatus report: Once all the servers have stopped, it should provide a suitable notification to the user.";
    protected final static int expectedArgNum = 0;

    public StopCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput, expectedArgNum);
    }

    @Override
    public void run(Object client, String[] tokens) throws Exception {
        super.run(client, tokens);
        ((ECSClient) client).stop();
        System.out.println("Servers stopped.");
    }
}
