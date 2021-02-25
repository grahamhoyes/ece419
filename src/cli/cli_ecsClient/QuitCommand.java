package cli.cli_ecsClient;

import app_kvECS.ECSClient;
import cli.AbstractCommand;

public class QuitCommand extends AbstractCommand {

    private final static String commandName = "quit";
    private final static String commandDescription = "" +
            "\tStops all server instances and exits the remote processes, then exits the CLI";
    private final static String commandParameters = "";
    private final static String commandOutput = "" +
            "\t\tstatus report: Notifies the user about the imminent program shutdown.";
    protected final static int expectedArgNum = 0;

    public QuitCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput, expectedArgNum);
    }

    @Override
    public void run(Object client, String[] tokens) throws Exception {
        super.run(client, tokens);
        if (((ECSClient) client).getECS().shutdown()) {
            System.out.println("All servers stopped");
        } else {
            System.out.println("Failed to shutdown some servers");
        }
        ((ECSClient) client).setRunning(false);
    }
}
