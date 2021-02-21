package cli.cli_ecsClient;

import app_kvECS.ECSClient;
import cli.AbstractCommand;

public class StartCommand extends AbstractCommand {

    private final static String commandName = "start";
    private final static String commandDescription = "" +
            "\tStarts the storage service by calling start() on all KVServer instances that participate in the service.";
    private final static String commandParameters = "";
    private final static String commandOutput = "" +
            "\t\tstatus report: Once all the instances have started, it should provide a suitable notification to the user.";
    protected final static int expectedArgNum = 0;

    public StartCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput, expectedArgNum);
    }

    @Override
    public void run(Object client, String[] tokens) throws Exception {
        super.run(client, tokens);
        boolean started = ((ECSClient) client).getECS().start();
        if (started) {
            System.out.println("Servers started.");
        } else {
            System.out.println("Failed to start all servers.");
        }
    }
}
