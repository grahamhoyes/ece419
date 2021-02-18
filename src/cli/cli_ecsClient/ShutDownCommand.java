package cli.cli_ecsClient;

import app_kvECS.ECSClient;
import cli.AbstractCommand;

public class ShutDownCommand extends AbstractCommand {

    private final static String commandName = "shutDown";
    private final static String commandDescription = "" +
            "\tStops all server instances and exits the remote processes..";
    private final static String commandParameters = "";
    private final static String commandOutput = "" +
            "\t\tstatus report: Notifies the user about the imminent program shutdown.";
    protected final static int expectedArgNum = 0;

    public ShutDownCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput, expectedArgNum);
    }

    @Override
    public void run(Object client, String[] tokens) throws Exception {
        super.run(client, tokens);
        ((ECSClient) client).shutdown();
        System.out.println("Process exited.");
    }
}
