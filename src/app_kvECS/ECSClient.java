package app_kvECS;

import cli.AbstractCommand;
import cli.UnrecognizedCommand;
import cli.cli_ecsClient.*;
import ecs.ECS;
import ecs.IECS;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ECSClient implements IECSClient, Runnable {

    private static final Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "> ";

    private IECS ecs;

    private boolean running;

    public ECSClient(String configFileName) throws IOException {
        ecs = new ECS(configFileName);
    }

    public static void main(String[] args) {
        try {
            new LogSetup("logs/ecs.log", Level.ALL);
            if (args.length == 0) {
                System.err.println("ECS needs to be initialized with a configuration file: `java -jar ECS.jar ecs.config`");
            } else {
                ECSClient client = new ECSClient(args[0]);
                new Thread(client).start();
            }
        } catch (IOException e) {
            System.err.println("Error! Unable to initialize logger.");
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    public IECS getECS() {
        return this.ecs;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public boolean isRunning() {
        return running;
    }

    private void handleCommand(String cmdLine){

        AbstractCommand command;
        String[] tokens = cmdLine.split("\\s+");

        if (tokens.length == 0) return;

        switch (tokens[0]) {
            case "addNodes":
                command = new AddNumberOfNodesCommand();
                break;
            case "start":
                command = new StartCommand();
                break;
            case "stop":
                command = new StopCommand();
                break;
            case "addNode":
                command = new AddNodeCommand();
                break;
            case "removeNode":
                command = new RemoveNodeCommand();
                break;
            case "shutDown":
            case "q":
            case "exit":
            case "quit":
                command = new ShutDownCommand();
                break;
            default:
                command = new UnrecognizedCommand();
                break;
        }

        try {
            command.run(this, tokens);
        } catch (Exception e) {
            this.printError(e.getMessage());
        }

    }

    public void printError(String message) {
        System.out.println(message);
    }


    @Override
    public void run() {
        this.running = true;
        BufferedReader stdin;

        while (isRunning()) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);
            try {
                String cmdLine = stdin.readLine();
                handleCommand(cmdLine);
            } catch (IOException e) {
                running = false;
                logger.error("CLI terminated");
            }
        }
    }
}
