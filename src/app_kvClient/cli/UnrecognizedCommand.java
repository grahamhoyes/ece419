package app_kvClient.cli;

import app_kvClient.KVClient;
import client.KVCommInterface;

public class UnrecognizedCommand extends AbstractCommand {

    private final static String commandName = "<anything else>";
    private final static String commandDescription = "Any unrecognized input in the context of this application.";
    private final static String commandParameters = " \t\n" +
            "\n" +
            "<any>";
    private final static String commandOutput = "error message: Unknown command;\n" +
            "\n" +
            "print the help text.";

    public UnrecognizedCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput);
    }

    @Override
    public void run(KVClient client, String[] tokens) {

    }
}
