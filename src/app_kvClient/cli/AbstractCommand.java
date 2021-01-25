package app_kvClient.cli;

import app_kvClient.KVClient;

public abstract class AbstractCommand {

    protected static String commandName;
    protected static String commandDescription;
    protected static String commandParameters;
    protected static String commandOutput;

    public AbstractCommand(String commandName, String commandDescription, String commandParameters, String commandOutput) {
        AbstractCommand.commandName = commandName;
        AbstractCommand.commandDescription = commandDescription;
        AbstractCommand.commandParameters = commandParameters;
        AbstractCommand.commandOutput = commandOutput;
    }

    public static String getCommandHelpDescription() {
        String helpDescription = "";

        helpDescription += commandName + ":\n";
        helpDescription += "\t" + commandDescription + "\n";
        if (commandParameters.length() != 0) {
            helpDescription += "\n\tParameters:\n";
            helpDescription += "\t\t" + commandParameters + "\n";
        }

        helpDescription += "\n\tOutput:\n";
        helpDescription += "\t\t" + commandOutput + "\n";

        return helpDescription;
    }

    public abstract void run(KVClient client, String[] args);

}
