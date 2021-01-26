package app_kvClient.cli;

import app_kvClient.KVClient;

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
    protected final static int expectedArgNum = 2;

    public PutCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput, expectedArgNum);
    }

    @Override
    public void run(KVClient client, String[] tokens) throws Exception {
        try {
            super.run(client, tokens);
            client.getStore().put(tokens[1], tokens[2]);
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
    }
}
