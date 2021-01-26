package app_kvClient.cli;

import app_kvClient.KVClient;
import client.KVCommInterface;

public class HelpCommand extends AbstractCommand {

    private final static String commandName = "help";
    private final static String commandDescription = "" +
            "\tShows the intended usage of the client application and describes its set of commands.";
    private final static String commandParameters = "";
    private final static String commandOutput = "" +
            "\t\thelp text: Shows the intended usage of the client application and describes its set of commands.";

    public HelpCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput);
    }

    @Override
    public void run(KVClient client, String[] tokens) {

        System.out.println(new ConnectCommand().getCommandHelpDescription());
        System.out.println(new DisconnectCommand().getCommandHelpDescription());
        System.out.println(new PutCommand().getCommandHelpDescription());
        System.out.println(new GetCommand().getCommandHelpDescription());
        System.out.println(new LogLevelCommand().getCommandHelpDescription());
        System.out.println(new HelpCommand().getCommandHelpDescription());
        System.out.println(new QuitCommand().getCommandHelpDescription());
        System.out.println(new UnrecognizedCommand().getCommandHelpDescription());

    }
}
