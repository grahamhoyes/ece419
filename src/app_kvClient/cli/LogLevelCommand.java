package app_kvClient.cli;

import app_kvClient.KVClient;
import org.apache.log4j.Level;

public class LogLevelCommand extends AbstractCommand {

    private final static String commandName = "logLevel <level>";
    private final static String commandDescription = "" +
            "\tSets the logger to the specified log level";
    private final static String commandParameters = "" +
            "\t\tlevel: One of the following log4j log levels: (ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF)";
    private final static String commandOutput = "" +
            "\t\tstatus message: Print out current log status.";

    public LogLevelCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput);
    }

    @Override
    public void run(KVClient client, String[] tokens) {
        if (tokens.length != 2) {
            client.printError("Invalid number of arguments. Usage: " + commandName);
            return;
        }

        client.setLogLevel(Level.toLevel(tokens[1]));
        System.out.println("Log level set to " + tokens[1]);
    }
}
