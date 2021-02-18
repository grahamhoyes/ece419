package cli.cli_kvClient;

import app_kvClient.KVClient;
import cli.AbstractCommand;
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

    private Level getLevel(String levelString) {

        if(levelString.equals(Level.ALL.toString())) {
            return Level.ALL;
        } else if(levelString.equals(Level.DEBUG.toString())) {
            return Level.DEBUG;
        } else if(levelString.equals(Level.INFO.toString())) {
            return Level.INFO;
        } else if(levelString.equals(Level.WARN.toString())) {
            return Level.WARN;
        } else if(levelString.equals(Level.ERROR.toString())) {
            return Level.ERROR;
        } else if(levelString.equals(Level.FATAL.toString())) {
            return Level.FATAL;
        } else if(levelString.equals(Level.OFF.toString())) {
            return Level.OFF;
        } else {
            return null;
        }
    }

    @Override
    public void run(KVClient client, String[] tokens) throws Exception {
        super.run(client, tokens);
        Level level = getLevel(tokens[1]);
        if (level == null) {
            throw new Exception("Invalid log level, must be one of: (ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF)");
        }
        client.setLogLevel(level);
        System.out.println("Log level set to " + tokens[1]);
    }
}
