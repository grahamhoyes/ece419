package app_kvClient.cli;

import app_kvClient.KVClient;
import client.KVCommInterface;

public class PutCommand extends AbstractCommand {

    private final static String commandName = "put <key> <value>";
    private final static String commandDescription = "\tInserts a key-value pair into the storage server data structures.\n" +
            "\tUpdates (overwrites) the current value with the given value if the server already contains the specified key.\n" +
            "\tDeletes the entry for the given key if <value> equals null.";
    private final static String commandParameters = "\t\tkey: arbitrary String (max length 20 Bytes)\n" +
            "\t\tvalue: arbitrary String (max. length 120 kByte)";
    private final static String commandOutput = "\t\tstatus message: provides a notification if the put- operation was successful (SUCCESS) or not (ERROR)";

    public PutCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput);
    }

    @Override
    public void run(KVClient client, String[] tokens) {

        try {
            client.getStore().put(tokens[1], tokens[2]);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}