package app_kvClient.cli;

import app_kvClient.KVClient;
import shared.messages.KVMessage;

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
            KVMessage message = client.getStore().put(tokens[1], null);
            switch (message.getStatus()) {
                case PUT_SUCCESS:
                case PUT_UPDATE:
                    System.out.println("Tuple was deleted successfully");
                    break;
                case PUT_ERROR:
                    throw new Exception(message.getMessage());
                default:
                    System.out.println("This should never happen?");
            }
        }
        else if (tokens.length == (expectedArgNum + 2)) {
            KVMessage message = client.getStore().put(tokens[1], tokens[2]);
            switch (message.getStatus()) {
                case PUT_SUCCESS:
                    System.out.println("Tuple was inserted successfully");
                    break;
                case PUT_UPDATE:
                    System.out.println("Tuple was updated successfully");
                    break;
                case PUT_ERROR:
                    throw new Exception(message.getMessage());
                default:
                    System.out.println("This should never happen?");
            }
        } else
            throw new Exception("Invalid number of arguments. Usage: " + commandName);
    }

}
