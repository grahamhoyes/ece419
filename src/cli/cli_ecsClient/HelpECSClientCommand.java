package cli.cli_ecsClient;

import cli.AbstractCommand;
import cli.UnrecognizedCommand;
import cli.cli_kvClient.HelpKVClientCommand;

public class HelpECSClientCommand extends AbstractCommand {

    private final static String commandName = "help";
    private final static String commandDescription = "" +
            "\tShows the intended usage of the ECS client application and describes its set of commands.";
    private final static String commandParameters = "";
    private final static String commandOutput = "" +
            "\t\thelp text: Shows the intended usage of the client application and describes its set of commands.";
    protected final static int expectedArgNum = 0;

    public HelpECSClientCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput, expectedArgNum);
    }

    public static void printHelp() {
        System.out.println(new AddNumberOfNodesCommand().getCommandHelpDescription());
        System.out.println(new StartCommand().getCommandHelpDescription());
        System.out.println(new StopCommand().getCommandHelpDescription());
        System.out.println(new ShutDownCommand().getCommandHelpDescription());
        System.out.println(new AddNodeCommand().getCommandHelpDescription());
        System.out.println(new RemoveNodeCommand().getCommandHelpDescription());
        System.out.println(new HelpECSClientCommand().getCommandHelpDescription());
        System.out.println(new UnrecognizedCommand().getCommandHelpDescription());
    }

    @Override
    public void run(Object client, String[] tokens) throws Exception {
        super.run(client, tokens);
        HelpKVClientCommand.printHelp();
    }
}
