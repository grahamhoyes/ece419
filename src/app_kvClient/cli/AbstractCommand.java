package app_kvClient.cli;

import app_kvClient.KVClient;

public abstract class AbstractCommand {

    protected String commandName;
    protected String commandDescription;
    protected String commandParameters;
    protected String commandOutput;
    protected int expectedArgNum;

    public AbstractCommand(String name, String desc, String params, String output, int num) {
        this.commandName = name;
        this.commandDescription = desc;
        this.commandParameters = params;
        this.commandOutput = output;
        this.expectedArgNum = num;
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

    public void run(KVClient client, String[] tokens) throws Exception {
        if (tokens.length != (this.expectedArgNum + 1)) {
            throw new Exception("Invalid number of arguments. Usage: " + commandName);
        }
    }

}
