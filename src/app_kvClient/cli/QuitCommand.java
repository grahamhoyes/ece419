package app_kvClient.cli;

import app_kvClient.KVClient;

public class QuitCommand extends AbstractCommand {

    private final static String commandName = "quit";
    private final static String commandDescription = "" +
            "\tTears down the active connection to the server and exits the program.";
    private final static String commandParameters = "";
    private final static String commandOutput = "" +
            "\t\tstatus report: Notifies the user about the imminent program shutdown.";
    protected final static int expectedArgNum = 0;

    public QuitCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput, expectedArgNum);
    }

    @Override
    public void run(KVClient client, String[] tokens) throws Exception {
        super.run(client, tokens);
        client.setRunning(false);
        System.out.println("Goodbye.");
    }
}
