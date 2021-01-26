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
    protected final static int expectedArgNum = 1;


    public LogLevelCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput, expectedArgNum);
    }

    @Override
    public void run(KVClient client, String[] tokens) throws Exception {
        try {
            super.run(client, tokens);
            client.setLogLevel(Level.toLevel(tokens[1]));
            System.out.println("Log level set to " + tokens[1]);
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
    }
}
