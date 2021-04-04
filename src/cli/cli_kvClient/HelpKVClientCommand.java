package cli.cli_kvClient;

import cli.AbstractCommand;
import cli.UnrecognizedCommand;

public class HelpKVClientCommand extends AbstractCommand {

    private final static String commandName = "help";
    private final static String commandDescription = "" +
            "\tShows the intended usage of the client application and describes its set of commands.";
    private final static String commandParameters = "";
    private final static String commandOutput = "" +
            "\t\thelp text: Shows the intended usage of the client application and describes its set of commands.";
    protected final static int expectedArgNum = 0;


    public HelpKVClientCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput, expectedArgNum);
    }

    public static void printHelp() {
        System.out.println(new ConnectCommand().getCommandHelpDescription());
        System.out.println(new DisconnectCommand().getCommandHelpDescription());
        System.out.println(new PutCommand().getCommandHelpDescription());
        System.out.println(new PutTTLCommand().getCommandHelpDescription());
        System.out.println(new GetCommand().getCommandHelpDescription());
        System.out.println(new LogLevelCommand().getCommandHelpDescription());
        System.out.println(new HelpKVClientCommand().getCommandHelpDescription());
        System.out.println(new QuitCommand().getCommandHelpDescription());
    }

    @Override
    public void run(Object client, String[] tokens) throws Exception {
        super.run(client, tokens);
        HelpKVClientCommand.printHelp();
    }
}
