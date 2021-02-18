package cli.cli_kvClient;

import app_kvClient.KVClient;
import cli.AbstractCommand;
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
    public void run(Object client, String[] tokens) throws Exception {
        super.run(client, tokens);
        KVMessage message = ((KVClient) client).getStore().get(tokens[1]);

        switch (message.getStatus()) {
            case GET_SUCCESS:
                System.out.println(message.getValue());
                break;
            case GET_ERROR:
                throw new Exception(message.getMessage());
            default:
                System.out.println("Something unexpected happened");
        }
    }
}
