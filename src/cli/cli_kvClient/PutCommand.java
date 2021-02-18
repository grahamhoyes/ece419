package cli.cli_kvClient;

import app_kvClient.KVClient;
import cli.AbstractCommand;
import shared.messages.KVMessage;

import java.util.Arrays;

public class PutCommand extends AbstractCommand {

    private final static String commandName = "put <key> <value>";
    private final static String commandDescription = "" +
            "\tInserts a key-value pair into the storage server data structures.\n" +
            "\tUpdates (overwrites) the current value with the given value if the server already contains the specified key.\n" +
            "\tDeletes the entry for the given key if <value> equals null.";
    private final static String commandParameters = "" +
            "\t\tkey: arbitrary String (max length 20 Bytes)\n" +
            "\t\tvalue: arbitrary String (max. length 120 kByte)";
    private final static String commandOutput = "" +
            "\t\tstatus message: provides a notification if the put- operation was successful (SUCCESS) or not (ERROR)";
    protected final static int expectedArgNum = 1;

    public PutCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput, expectedArgNum);
    }

    @Override
    public void run(KVClient client, String[] tokens) throws Exception {

        if (tokens.length == (expectedArgNum + 1)) {
            KVMessage message = client.getStore().put(tokens[1], "null");
            switch (message.getStatus()) {
                case DELETE_SUCCESS:
                    System.out.printf("Tuple with key \"%s\" was deleted successfully%n", message.getKey());
                    break;
                case DELETE_ERROR:
                    throw new Exception(message.getMessage());
                default:
                    System.out.println("Something unexpected happened");
            }
        }
        else if (tokens.length >= (expectedArgNum + 2)) {
            String v = String.join(" ", Arrays.copyOfRange(tokens, 2, tokens.length));
            KVMessage message = client.getStore().put(tokens[1], v);
            switch (message.getStatus()) {
                case PUT_SUCCESS:
                    System.out.printf("Tuple {%s, %s} was inserted successfully%n", message.getKey(), message.getValue());
                    break;
                case PUT_UPDATE:
                    System.out.printf("Tuple was updated successfully to {%s, %s}%n", message.getKey(), message.getValue());
                    break;
                case DELETE_SUCCESS:
                    System.out.printf("Tuple with key \"%s\" was deleted successfully%n", message.getKey());
                    break;
                case PUT_ERROR:
                    System.out.println("Error: put failed");
                    break;
                case DELETE_ERROR:
                    System.out.printf("Error: key does not exist: %s", message.getKey());
                    break;
                default:
                    System.out.println("Something unexpected happened");
            }
        } else
            throw new Exception("Invalid number of arguments. Usage: " + commandName);
    }

}
