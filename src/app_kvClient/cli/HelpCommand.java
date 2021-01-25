package app_kvClient.cli;

import app_kvClient.KVClient;
import client.KVCommInterface;

public class HelpCommand extends AbstractCommand {

    private final static String commandName = "help";
    private final static String commandDescription = "Shows the intended usage of the client application and describes its set of commands.";
    private final static String commandParameters = "";
    private final static String commandOutput = "help text: Shows the intended usage of the client application and describes its set of commands.";

    public HelpCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput);
    }

    @Override
    public void run(KVClient client, String[] tokens) {

        System.out.println(ConnectCommand.getCommandHelpDescription() + "\n");
        System.out.println(DisconnectCommand.getCommandHelpDescription() + "\n");
        System.out.println(PutCommand.getCommandHelpDescription() + "\n");
        System.out.println(GetCommand.getCommandHelpDescription() + "\n");

    }
}
