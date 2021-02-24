package testing;

import app_kvClient.KVClient;
import app_kvECS.ECSClient;
import app_kvServer.KVServer;
import client.KVStoreConnection;
import ecs.ECS;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.MalformedInputException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.CyclicBarrier;

public class PerformanceTestM2 {

    private static final Logger logger = Logger.getRootLogger();

    String hostname = "127.0.0.1";
    int port = 8000;

    private ECS ecs;
    private ArrayList<ClientThread> clients;

    private ArrayList<ArrayList<String>> data;

    private final int[] numbersToTest = {2, 4, 8};

    public PerformanceTestM2(String dataPath) {
        data = readData(dataPath);
    }

    public void listFiles(String originalDataPath, String folder, ArrayList<ArrayList<String>> data) throws IOException {
        File directory = new File(folder);
        File[] contents = directory.listFiles();
        assert contents != null;
        for (File f : contents) {
            if (f.isDirectory()) {
                listFiles(originalDataPath, f.getPath(), data);
            } else {
                String key = f.getPath().substring(originalDataPath.length());
                String value;
                try {
                    value = Files.readString(f.toPath(), Charset.defaultCharset());
                }
                catch (MalformedInputException e) {
                    continue;
                }
                // no tuples in java, why you do this to me
                ArrayList<String> kvPair = new ArrayList<>();
                kvPair.add(key);
                kvPair.add(value);
                data.add(kvPair);
            }
        }
    }


    public ArrayList<ArrayList<String>> readData(String dataPath) {
        ArrayList<ArrayList<String>> data = new ArrayList<>();

        try {
            listFiles(dataPath, dataPath, data);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return data;
    }

    public void reset(String ecsConfigFilePath, String zkHost, int zkPort, String remotePath, int numServers, int numClients) {
        if (ecs != null) ecs.shutdown();

        ecs = new ECS(ecsConfigFilePath, zkHost, zkPort, remotePath);
        ecs.addNodes(numServers);
        ecs.start();

        clients = new ArrayList<>();
        int stride = (data.size() / numClients);
        for (int i=0; i<numClients; i++)
            clients.add(new ClientThread(hostname, port, data.subList(i*stride, (i+1)*stride)));
    }

    public void runTest(String ecsConfigFilePath, String zkHost, int zkPort, String remotePath) {
        for (int numClients : numbersToTest) {
            for (int numServers : numbersToTest) {

                reset(ecsConfigFilePath, zkHost, zkPort, remotePath, numServers, numClients);

                ArrayList<Thread> threads = new ArrayList<>();
                for (int iClient = 0; iClient < numClients; iClient++) {
                    Thread t = new Thread(clients.get(iClient));
                    t.start();
                    threads.add(t);
                }

                for (Thread t: threads) {
                    try {
                        t.join();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }
        }
    }

    public static void main(String[] args) {
        try {
            new LogSetup("logs/performance.log", Level.ALL);
        } catch (IOException e) {
            e.printStackTrace();
        }
        new PerformanceTestM2(args[4]).runTest(args[0], args[1], Integer.parseInt(args[2]), args[3]);
    }

    public class ClientThread implements Runnable {

        private final List<ArrayList<String>> assignedData;
        private final KVStoreConnection storeConnection;

        public ClientThread(String hostname, int port, List<ArrayList<String>> assignedData) {
            this.storeConnection = new KVStoreConnection(hostname, port);
            this.assignedData = assignedData;
        }

        @Override
        public void run() {
            try {
                storeConnection.connect();

                for (ArrayList<String> kvPair : assignedData) {
                    String key = kvPair.get(0);
                    String value = kvPair.get(1);
                    storeConnection.put(key, value);
                }

                storeConnection.disconnect();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
