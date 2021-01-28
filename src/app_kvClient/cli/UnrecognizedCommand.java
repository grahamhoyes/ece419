package app_kvClient.cli;

import app_kvClient.KVClient;

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
    public void run(KVClient client, String[] tokens) throws Exception {
        if (tokens.length == 0 || (tokens.length == 1 && tokens[0].equals(""))) {
            return;
        }
        throw new Exception("Unknown command");
    }
}
