package app_kvClient.cli;

import app_kvClient.KVClient;
import shared.messages.KVMessage;

public class GetCommand extends AbstractCommand {

    private final static String commandName = "get <key>";
    private final static String commandDescription = "" +
            "\tRetrieves the value for the given key from the storage server.";
    private final static String commandParameters = "" +
            "\t\tkey: the key that indexes the desired value (max length 20 Bytes)";
    private final static String commandOutput = "" +
            "\t\tvalue: the value that is indexed by the given key if present at the storage server, or an error message if the value for the given key is not present.";
    protected final static int expectedArgNum = 1;


    public GetCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput, expectedArgNum);
    }

    @Override
    public void run(KVClient client, String[] tokens) throws Exception {
        super.run(client, tokens);
        KVMessage message = client.getStore().get(tokens[1]);
        System.out.println(message.serialize());
    }
}