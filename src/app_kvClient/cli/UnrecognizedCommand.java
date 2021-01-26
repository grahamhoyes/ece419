package app_kvClient.cli;

import app_kvClient.KVClient;
import client.KVCommInterface;

public class UnrecognizedCommand extends AbstractCommand {

    private final static String commandName = "<anything else>";
    private final static String commandDescription = "" +
            "\tAny unrecognized input in the context of this application.";
    private final static String commandParameters = "" +
            "\t\t<any>";
    private final static String commandOutput = "" +
            "\t\terror message: Unknown command";

    public UnrecognizedCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput);
    }

    @Override
    public void run(KVClient client, String[] tokens) {

    }
}
