package cli.cli_ecsClient;

import app_kvECS.ECSClient;
import cli.AbstractCommand;
import ecs.ECS;

public class SettingsCommand extends AbstractCommand  {
    private final static String commandName = "set <setting> <value>";
    private final static String commandDescription = "" +
            "\tSets the given configuration option";
    private final static String commandParameters = "" +
            "\t\tsettings:" +
            "\t\t\treplicatorsExpireKeys: 'on' or 'off' to allow replicators to expire keys relative to their local clocks";
    private final static String commandOutput = "" +
            "\t\tstatus message: provides a notification if the settings update was successful or not";
    protected final static int expectedArgNum = 2;

    public SettingsCommand() {
        super(commandName, commandDescription, commandParameters, commandOutput, expectedArgNum);
    }

    @Override
    public void run(Object client, String[] tokens) throws Exception {
        ECS ecs = ((ECSClient) client).getECS();

        boolean success;

        if (tokens[1].equals("replicatorsExpireKeys")) {
            if (tokens[2].equals("on")) {
                success = ecs.setReplicatorsExpireKeys(true);
            } else if (tokens[2].equals("off")) {
                success = ecs.setReplicatorsExpireKeys(false);
            } else {
                throw new Exception("Invalid value " + tokens[2]);
            }

            if (success) {
                System.out.println("Setting updated");
            } else {
                System.out.println("Something went wrong updating the setting");
            }

        } else {
            System.err.println("Invalid setting");
        }
    }
}
