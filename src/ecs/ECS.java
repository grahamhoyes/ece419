package ecs;

import app_kvServer.IKVServer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import shared.messages.AdminMessage;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ECS implements IECS {
    private static final Logger logger = Logger.getRootLogger();
    private static final String SERVER_JAR = "KVServer.jar";

    // ZooKeeper is assumed to be running on the default port
    // on this machine
    private static final String ZK_HOST = "127.0.0.1";
    private static final int ZK_PORT = 2181;

    private static final String cacheStrategy = "";
    private static final int cacheSize = 0;

    String remotePath;
    private final ZooKeeperConnection zkConnection;
    private ZooKeeper zk;

    // Map server name to ECSNode
    private HashMap<String, ECSNode> configMap = new HashMap<>();

    // Queue of inactive ECSNodes
    private Queue<ECSNode> nodePool = new LinkedList<>();

    // Set of active nodes
    private HashRing hashRing;

    public ECS(String configFileName, String remotePath) {
        this.remotePath = remotePath;

        // Read in the config file
        try {
            File configFile = new File(configFileName);
            if (!configFile.exists()) {
                logger.fatal("ECS config file does not exist");
                System.exit(1);
            }

            Scanner sc = new Scanner(configFile);

            String line;

            while (sc.hasNextLine()) {
                line = sc.nextLine();
                if (line.strip().equals("")) continue;

                String[] tokens = line.split(" ");
                if (tokens.length != 3) {
                    logger.warn("Invalid config entry: " + line);
                    continue;
                }

                String name = tokens[0];
                String host = tokens[1];
                String port = tokens[2];

                if (configMap.containsKey(name)) {
                    logger.error(name + "already exists");
                    continue;
                }

                ECSNode node = new ECSNode(name, host, Integer.parseInt(port));
                configMap.put(name, node);
                nodePool.add(node);
            }
        } catch (IOException e) {
            logger.fatal("Failed to read ECS config file");
            System.exit(1);
        }


        hashRing = new HashRing();

        zkConnection = new ZooKeeperConnection();

        try {
            zk = zkConnection.connect(ZK_HOST, ZK_PORT);
        } catch (InterruptedException | IOException e) {
            logger.fatal("Failed to establish a connection to ZooKeeper");
            e.printStackTrace();
            System.exit(1);
        }

        // Create the server root, heartbeat, and metadata nodes if they don't exist
        try {
            zkConnection.createOrReset(ZooKeeperConnection.ZK_SERVER_ROOT, "root", CreateMode.PERSISTENT);
            zkConnection.createOrReset(ZooKeeperConnection.ZK_HEARTBEAT_ROOT, "heartbeat", CreateMode.PERSISTENT);
            zkConnection.createOrReset(ZooKeeperConnection.ZK_METADATA_PATH, hashRing.serialize(), CreateMode.PERSISTENT);
        } catch (InterruptedException | KeeperException e) {
            logger.fatal("Unable to create ZNode");
            e.printStackTrace();
            System.exit(1);
        }

        addNodes(1);

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

    @Override
    public ECSNode addNode() {
        return addNode(cacheStrategy, cacheSize);
    }

    @Override
    public ECSNode addNode(String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public Collection<ECSNode> addNodes(int count) {
        return addNodes(count, cacheStrategy, cacheSize);
    }

    @Override
    public Collection<ECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        Collection<ECSNode> nodes = setupNodes(count, cacheStrategy, cacheSize);
        if (nodes == null) return null;

        for (ECSNode node : nodes) {
            String javaCmd = String.join(" ",
                    "java -jar",
                    remotePath + SERVER_JAR,
                    String.valueOf(node.getNodePort()),
                    node.getNodeName(),
                    ZK_HOST,
                    String.valueOf(ZK_PORT));

            boolean isLocal = node.getNodeHost().equals("127.0.0.1") || node.getNodeHost().equals("localhost");

            String cmd;

            if (isLocal) {
                cmd = javaCmd;
            } else {
                // TODO: Test this. The & at the end may be an issue
                cmd = String.join(" ",
                        "ssh -o StrictHostsKeyChecking=no -n",
                        node.getNodeHost(),
                        "nohup",
                        javaCmd,
                        "&");
            }

            try {
                System.out.println(cmd);
                Process p = Runtime.getRuntime().exec(cmd);

                if (isLocal) {
                    logger.info("Local KVServer started with process " + p.pid());
                } else {
                    logger.info("Remote KVServer started on " + node.getNodeHost() + ":" + node.getNodePort());
                }
            } catch (IOException e) {
                logger.error("Unable to launch node " + node.getNodeName() + " on host " + node.getNodeHost(), e);
                nodes.remove(node);
                continue;
            }

            // After initialization, nodes should create their ZNodes.
            // Watch for the heartbeat to come online.

            // Signal for successful connections, to synchronize otherwise async watchers
            CountDownLatch sig = new CountDownLatch(1);

            String zkPath = ZooKeeperConnection.ZK_SERVER_ROOT + "/" + node.getNodeName();
            String zkAdminPath = zkPath + "/admin";

            try {
                zk.exists(zkAdminPath, event -> sig.countDown());

                boolean success = sig.await(5000, TimeUnit.MILLISECONDS);

                if (!success) {
                    logger.error("Timeout while waiting to start server " + node.getNodeName());
                    nodes.remove(node);
                    continue;
                }

            } catch (KeeperException | InterruptedException e) {
                logger.error("Error waiting for heartbeat thread for server " + node.getNodeName());
                nodes.remove(node);
                continue;
            }

            logger.info("Server " + node.getNodeName() + " has been started");

            // Initialize the server
            AdminMessage adminMessage = new AdminMessage(AdminMessage.Action.INIT);
            adminMessage.setNodeMetadata(node);

            CountDownLatch adminWait = new CountDownLatch(1);

            try {
                zkConnection.createOrReset(zkAdminPath, adminMessage.serialize(), CreateMode.PERSISTENT);

                // Server will respond by setting an ack status on its ZNode
                zk.getData(zkPath, event -> {
                    try {
                        byte[] data = zk.getData(zkPath, false, null);
                        AdminMessage message = new AdminMessage(new String(data));

                        if (message.getAction() == AdminMessage.Action.ACK) {
                            logger.info("Server " + node.getNodeName() + " initialized");
                        } else {
                            logger.error("Server " + node.getNodeName() + " failed to initialize");
                            logger.debug(message.getMessage());
                        }

                        adminWait.countDown();
                    } catch (KeeperException | InterruptedException e) {
                        logger.error("Failed to receive admin message", e);
                    }
                }, null);

                boolean success = adminWait.await(10000, TimeUnit.MILLISECONDS);

                if (!success) {
                    logger.error("Timeout waiting to initialize node " + node.getNodeName());
                    nodes.remove(node);
                }

            } catch (KeeperException | InterruptedException e) {
                logger.error("Failed to send admin message", e);
                nodes.remove(node);
            }

        }

        // nodes is now everything that was successfully initialized
        for (ECSNode node: nodes) {
            hashRing.addNode(node);
        }

        return nodes;
    }

    @Override
    public Collection<ECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        if (count > nodePool.size()) return null;

        List<ECSNode> nodes = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ECSNode node = nodePool.poll();
            assert node != null;
            node.setStatus(IKVServer.ServerStatus.STOPPED);
            nodes.add(node);
        }
        // Setting up ZNodes is handled by the client

        // Metadata (informing all nodes of all others) isn't set until after
        // nodes have been successfully added

        return nodes;
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
    public Map<String, ECSNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public ECSNode getNodeByKey(String Key) {
        // TODO
        return null;
    }

}
