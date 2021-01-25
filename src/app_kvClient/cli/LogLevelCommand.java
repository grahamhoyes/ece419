package app_kvClient.cli;

import app_kvClient.KVClient;
import client.KVCommInterface;

public class LogLevelCommand extends AbstractCommand {

    private final static String commandName = "logLevel <level>";
    private final static String commandDescription = "\tSets the logger to the specified log level";
    private final static String commandParameters = "\t\tlevel: One of the following log4j log levels:\n" +
            "\t\t\t(ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF)";
    private final static String commandOutput = "\t\tstatus message: Print out current log status.";

    public LogLevelCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput);
    }

    @Override
    public void run(KVClient client, String[] tokens) {

    }
}
