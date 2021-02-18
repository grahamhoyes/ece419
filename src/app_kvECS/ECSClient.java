package app_kvECS;

import cli.AbstractCommand;
import cli.UnrecognizedCommand;
import cli.cli_ecsClient.*;
import ecs.IECSNode;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Map;

public class ECSClient implements IECSClient, Runnable {


    private static final Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "> ";

    private static final String cacheStrategy = "";
    private static final int cacheSize = 0;

    private boolean running;

    public ECSClient(String configFileName) throws IOException {
//        ecs = new ECS(configFileName);
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

    public void setLogLevel(Level level) {
        logger.setLevel(level);
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean start() {
        // TODO
        return false;
    }

    @Override
    public boolean stop() {
        // TODO
        return false;
    }

    @Override
    public boolean shutdown() {
        // TODO
        return false;
    }

    public IECSNode addNode() {
        return addNode(cacheStrategy, cacheSize);
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    public Collection<IECSNode> addNodes(int count) {
        return addNodes(count, cacheStrategy, cacheSize);
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
        return false;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
        return null;
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
