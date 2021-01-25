package app_kvClient.cli;

import app_kvClient.KVClient;

public abstract class AbstractCommand {

    protected String commandName;
    protected String commandDescription;
    protected String commandParameters;
    protected String commandOutput;

    public AbstractCommand(String commandName, String commandDescription, String commandParameters, String commandOutput) {
        this.commandName = commandName;
        this.commandDescription = commandDescription;
        this.commandParameters = commandParameters;
        this.commandOutput = commandOutput;
    }

    public String getCommandHelpDescription() {
        String helpDescription = "";

        helpDescription += this.commandName + ":\n";
        helpDescription += this.commandDescription + "\n";
        if (this.commandParameters.length() != 0) {
            helpDescription += "\tParameters:\n";
            helpDescription += this.commandParameters + "\n";
        }

        helpDescription += "\tOutput:\n";
        helpDescription += this.commandOutput + "\n";

        return helpDescription;
    }

    public abstract void run(KVClient client, String[] args);

}
