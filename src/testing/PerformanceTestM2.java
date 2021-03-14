package testing;

import client.KVStoreConnection;
import ecs.ECS;
import ecs.ServerNode;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.MalformedInputException;
import java.nio.file.Files;
import java.util.*;

public class PerformanceTestM2 {

    private static final Logger logger = Logger.getRootLogger();

    String hostname = "127.0.0.1";
    int port = 8000;

    private ECS ecs;
    private ArrayList<ClientThread> clients;

    private List<ArrayList<String>> data;
    private final List<ArrayList<String>> workingData;

    private static final int numRequests = 500;

    private int numServers;
    private int numClients;
    private double ratio;


    public PerformanceTestM2(String ecsConfigFilePath, String zkHost, int zkPort, String remotePath, String dataPath, int numServers, int numClients, double ratio) {
        this.numServers = numServers;
        this.numClients = numClients;
        this.ratio = ratio;

        data = readData(dataPath);
        workingData = data.subList(0, numRequests);
        data = data.subList(numRequests, numRequests*2);

        ecs = new ECS(ecsConfigFilePath, zkHost, zkPort, remotePath);
        ecs.addNodes(numServers);
        populateStorage(numClients);

        clients = new ArrayList<>();
        int stride = (data.size() / numClients);
        for (int i=0; i<numClients; i++)
            clients.add(new ClientThread(hostname, port, workingData.subList(i*stride, (i+1)*stride), false, ratio));

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

    public void runClients(ArrayList<ClientThread> clients) {
        ArrayList<Thread> threads = new ArrayList<>();
        for (ClientThread client : clients) {
            Thread t = new Thread(client);
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

    public void populateStorage(int numClients) {
        clients = new ArrayList<>();
        int stride = (data.size() / numClients);
        for (int i=0; i<numClients; i++)
            clients.add(new ClientThread(hostname, port, data.subList(i*stride, (i+1)*stride), true, 0));

        runClients(clients);
    }

    public void evaluateRun(long timeTaken, long timeToAddANode, long timeToRemoveANode) {
        System.out.println(numServers);
        System.out.println(numClients);
        System.out.println(ratio);

        int numWrites = 0;
        int numReads = 0;

        for (ClientThread client: clients) {
            numWrites += client.numWriteRequests;
            numReads += client.numReadRequests;
        }

        System.out.println(numWrites);
        System.out.println(numReads);
        System.out.println(timeTaken);
        System.out.println(timeToAddANode);
        System.out.println(timeToRemoveANode);
    }

    public void runTest() {
        long start = System.nanoTime();
        runClients(clients);
        long end = System.nanoTime();
        long timeTaken = (end - start) / 1000;

        start = System.nanoTime();
        ServerNode node = ecs.addNode();
        end = System.nanoTime();
        long timeToAddANode = (end - start) / 1000;

        ecs.removeNode(node.getNodeName());

        start = System.nanoTime();
        for (ServerNode nodeToRemove: ecs.getNodes()) {
            ecs.removeNode(nodeToRemove.getNodeName());
            break;
        }
        end = System.nanoTime();
        long timeToRemoveANode = (end - start) / 1000;

        evaluateRun(timeTaken, timeToAddANode, timeToRemoveANode);
        ecs.shutdown();
    }

    public static void main(String[] args) {
        try {
            new LogSetup("logs/performance.log", Level.OFF);
            Logger.getLogger("Connection").setLevel(Level.OFF);
            Logger.getLogger("ClientConnection").setLevel(Level.OFF);
            Logger.getLogger("KVServer").setLevel(Level.OFF);
            Logger.getLogger("ECSConnection").setLevel(Level.OFF);
            Logger.getLogger("ECS").setLevel(Level.OFF);
            Logger.getLogger("KVSimpleStore").setLevel(Level.OFF);
            Logger.getLogger("ZooKeeperConnection").setLevel(Level.OFF);
        } catch (IOException e) {
            e.printStackTrace();
        }
        new PerformanceTestM2(args[0], args[1], Integer.parseInt(args[2]), args[3], args[4], Integer.parseInt(args[5]), Integer.parseInt(args[6]), Double.parseDouble(args[7])).runTest();
    }

    public class ClientThread implements Runnable {

        private final List<ArrayList<String>> assignedData;
        private final KVStoreConnection storeConnection;
        private final boolean populating;
        private final double ratio;

        long totalTimeWrite = 0;
        long totalTimeRead = 0;
        int numReadRequests = 0;
        int numWriteRequests = 0;

        Random ran = new Random();

        public ClientThread(String hostname, int port, List<ArrayList<String>> assignedData, boolean populating, double ratio) {
            this.storeConnection = new KVStoreConnection(hostname, port);
            this.assignedData = assignedData;
            this.populating = populating;
            this.ratio = ratio;
        }

        public void sendPutRequest(String key, String value) {
            long start = System.nanoTime();
            try {
                storeConnection.put(key, value);
            } catch (Exception ignored) {
            }
            long end = System.nanoTime();
            totalTimeWrite += (end - start) / 1000;
            numWriteRequests++;
        }

        public void sendGetRequest(String key) {
            long start = System.nanoTime();
            try {
                storeConnection.get(key);
            } catch (Exception ignored) {
            }
            long end = System.nanoTime();
            totalTimeRead += (end - start) / 1000;
            numReadRequests++;
        }

        @Override
        public void run() {
            try {
                storeConnection.connect();

                for (ArrayList<String> kvPair : assignedData) {
                    String key = kvPair.get(0);
                    String value = kvPair.get(1);

                    if (populating) {
                        storeConnection.put(key, value);
                    }
                    else {
                        if (Math.random() < ratio)
                            sendPutRequest(key, value);
                        else {
                            int randomIndex = ran.nextInt(data.size());
                            ArrayList<String> pair = data.get(randomIndex);

                            sendGetRequest(pair.get(0));
                        }
                    }

                    if (numReadRequests + numWriteRequests == numRequests) {
                        break;
                    }
                }

                storeConnection.disconnect();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
