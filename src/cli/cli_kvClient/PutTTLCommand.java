package cli.cli_kvClient;

import app_kvClient.KVClient;
import cli.AbstractCommand;
import client.KVCommInterface;
import shared.messages.KVMessage;

import java.util.Arrays;
import java.util.Date;

public class PutTTLCommand extends AbstractCommand {

    private final static String commandName = "put <ttl> <key> <value>";
    private final static String commandDescription = "" +
            "\tInserts a key-value pair into the storage server data structures.\n" +
            "\tUpdates (overwrites) the current value with the given value if the server already contains the specified key.\n" +
            "\tThe key-value pair will expire after a specified number of seconds.";
    private final static String commandParameters = "" +
            "\t\tttl: positive Integer, number of seconds\n" +
            "\t\tkey: arbitrary String (max length 20 Bytes)\n" +
            "\t\tvalue: arbitrary String (max. length 120 kByte)";
    private final static String commandOutput = "" +
            "\t\tstatus message: provides a notification if the put- operation was successful (SUCCESS) or not (ERROR)";
    protected final static int expectedArgNum = 3;

    public PutTTLCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput, expectedArgNum);
    }

    @Override
    public void run(Object client, String[] tokens) throws Exception {
        KVCommInterface store = ((KVClient) client).getStore();
        if (store == null) {
            System.out.println("Not connected to any store");
            return;
        }

         if (tokens.length >= expectedArgNum) {
            String v = String.join(" ", Arrays.copyOfRange(tokens, 3, tokens.length));
            KVMessage message = store.putTTL(tokens[2], v, Long.valueOf(tokens[1]));

             Date date = new Date ();
            switch (message.getStatus()) {
                case PUT_SUCCESS:
                    date.setTime(message.getTTL());
                    System.out.printf("Tuple {%s, %s} was inserted successfully and will be expired on %s%n", message.getKey(), message.getValue(), date);
                    break;
                case PUT_UPDATE:
                    date.setTime(message.getTTL());
                    System.out.printf("Tuple was updated successfully to {%s, %s} and will be expired on %s%n", message.getKey(), message.getValue(), date);
                    break;
                case PUT_ERROR:
                    System.out.println("Error: put failed");
                    break;
                case SERVER_STOPPED:
                    System.out.println("Server is stopped. Please try again.");
                    break;
                case SERVER_WRITE_LOCK:
                    System.out.println("Server is write locked. Please try again.");
                    break;
                default:
                    System.out.println("Something unexpected happened");
            }
        } else
            throw new Exception("Invalid number of arguments. Usage: " + commandName);
    }

}
