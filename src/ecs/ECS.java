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
import java.util.concurrent.TimeoutException;

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
    }

    @Override
    public boolean start() {
        boolean startedAll = true;
        for (ECSNode node : hashRing.getNodes()) {
            AdminMessage message = new AdminMessage(AdminMessage.Action.START);
            try {
                AdminMessage response = zkConnection.sendAdminMessage(node.getNodeName(), message, 10000);
                if (response.getAction() == AdminMessage.Action.ACK) {
                    logger.info("Started server: " + node.getNodeName());
                } else {
                    logger.error("Could not start server: " + node.getNodeName());
                    startedAll = false;
                }
            } catch (KeeperException | InterruptedException e) {
                logger.error("Failed to start server: " + node.getNodeName(), e);
                startedAll = false;
            } catch (TimeoutException e) {
                logger.error("Timeout while trying to start server: " + node.getNodeName());
                startedAll = false;
            }
        }
        return startedAll;
    }

    @Override
    public boolean stop() {
        boolean stoppedAll = true;
        for (ECSNode node : hashRing.getNodes()) {
            AdminMessage message = new AdminMessage(AdminMessage.Action.STOP);
            try {
                AdminMessage response = zkConnection.sendAdminMessage(node.getNodeName(), message, 10000);
                if (response.getAction() == AdminMessage.Action.ACK) {
                    logger.info("Stopped server: " + node.getNodeName());
                } else {
                    logger.error("Could not stop server: " + node.getNodeName());
                    stoppedAll = false;
                }
            } catch (KeeperException | InterruptedException e) {
                logger.error("Failed to stop server: " + node.getNodeName(), e);
                stoppedAll = false;
            } catch (TimeoutException e) {
                logger.error("Timeout while trying to stop server: " + node.getNodeName());
                stoppedAll = false;
            }
        }
        return stoppedAll;
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
        List<ECSNode> nodes = (List<ECSNode>) addNodes(1, cacheStrategy, cacheSize);
        if (nodes.size() > 0)
            return nodes.get(0);

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

        HashRing updatedHashRing = hashRing.copy();
        Collection<ECSNode> launchedNodes = new ArrayList<>();

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
                Process p = Runtime.getRuntime().exec(cmd);

                if (isLocal) {
                    logger.info("Local KVServer started with process " + p.pid());
                } else {
                    logger.info("Remote KVServer started on " + node.getNodeHost() + ":" + node.getNodePort());
                }
            } catch (IOException e) {
                logger.error("Unable to launch node " + node.getNodeName() + " on host " + node.getNodeHost(), e);
                continue;
            }

            String zkHeartbeatPath = ZooKeeperConnection.ZK_HEARTBEAT_ROOT + "/" + node.getNodeName();
            AdminMessage adminMessage = new AdminMessage(AdminMessage.Action.NOP);

            // Signal for successful connections, to synchronize otherwise async watchers
            CountDownLatch sig = new CountDownLatch(1);

            // Watch for the heartbeat to come online.
            try {
                zk.exists(zkHeartbeatPath, event -> sig.countDown());

                boolean success = sig.await(5000, TimeUnit.MILLISECONDS);

                if (!success) {
                    logger.error("Timeout while waiting to start server " + node.getNodeName());
                    continue;
                }

            } catch (KeeperException | InterruptedException e) {
                logger.error("Error waiting for heartbeat thread for server " + node.getNodeName());
                continue;
            }

            logger.info("Server " + node.getNodeName() + " has been started");

            // Initialize the server
            adminMessage.setAction(AdminMessage.Action.INIT);

            try {
                AdminMessage response = zkConnection.sendAdminMessage(
                        node.getNodeName(), adminMessage, 10000
                );

                if (response.getAction() == AdminMessage.Action.ACK) {
                    logger.info("Server " + node.getNodeName() + " initialized");
                } else {
                    logger.error("Server " + node.getNodeName() + " failed to initialize");
                    logger.debug(response.getMessage());
                    continue;
                }
            } catch (KeeperException | InterruptedException e) {
                logger.error("Failed to send admin message to initialize " + node.getNodeName(), e);
                continue;
            } catch (TimeoutException e) {
                logger.error("Timeout while trying to send admin message to initialize " + node.getNodeName());
                continue;
            }

            launchedNodes.add(node);
            updatedHashRing.addNode(node);

        }

        boolean retry = true;
        while (retry) {  // Retry until all nodes have been successfully added
            retry = false;

            // Set the metadata for the added nodes
            for (ECSNode node : launchedNodes) {
                AdminMessage message = new AdminMessage(AdminMessage.Action.SET_METADATA);
                message.setMetadata(updatedHashRing);

                try {
                    AdminMessage response = zkConnection.sendAdminMessage(node.getNodeName(), message, 10000);

                    if (response.getAction() == AdminMessage.Action.ACK) {
                        logger.info("Set metadata for server " + node.getNodeName());
                    } else {
                        logger.error("Could not set metadata for server " + node.getNodeName());
                        launchedNodes.remove(node);
                        updatedHashRing.removeNode(node.getNodeName());
                        retry = true;
                        break;
                    }

                // Errors mean that we have to remove this node from the hash ring, and retry
                } catch (KeeperException | InterruptedException e) {
                    logger.error("Failed to send admin message to set metadata for " + node.getNodeName(), e);
                    launchedNodes.remove(node);
                    updatedHashRing.removeNode(node.getNodeName());
                    retry = true;
                    break;
                } catch (TimeoutException e) {
                    logger.error("Timeout while trying to send admin message to set metadata for " + node.getNodeName());
                    launchedNodes.remove(node);
                    updatedHashRing.removeNode(node.getNodeName());
                    retry = true;
                    break;
                }

            }
        }

        // For each node that has been launched so far, invoke data transfer to its successor
        for (ECSNode node : launchedNodes) {
            ECSNode successor = updatedHashRing.getSuccessor(node);
            assert successor != null;

            // Set the write lock on the successor
            // TODO: Handle failures here and abort the transfer
            if (!setWriteLock(successor, true)) continue;

            // Invoke data transfer
            moveDataBetweenNodes(successor, node);

        }

        // Once all data has been transferred, send global metadata updates
        // TODO: Does this have to be synchronous before the next step? We don't get responses back
        hashRing = updatedHashRing;

        try {
            zkConnection.setData(ZooKeeperConnection.ZK_METADATA_PATH, hashRing.serialize());
        } catch (KeeperException | InterruptedException e) {
            logger.error("Failed to update global metadata", e);
        }

        // Release the write lock on the successor servers and remove old data
        for (ECSNode node : launchedNodes) {
            ECSNode successor = updatedHashRing.getSuccessor(node);
            assert successor != null;

            setWriteLock(successor, false);
        }

        logger.info("Successfully launched " + launchedNodes.size() + " nodes, "
                + (count - launchedNodes.size()) + " failures");

        return launchedNodes;
    }

    @Override
    public Collection<ECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        if (count > nodePool.size()) return null;

        List<ECSNode> nodes = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ECSNode node = nodePool.poll();
            assert node != null;
            node.setStatus(IKVServer.ServerStatus.STOPPED);

            // Before bringing up the node, create ZNodes for it. We don't care
            // if these stick around if the node fails to launch for some reason
            String zkNodePath = ZooKeeperConnection.ZK_SERVER_ROOT + "/" + node.getNodeName();
            String zkAdminPath = zkNodePath + "/admin";

            AdminMessage adminMessage = new AdminMessage(AdminMessage.Action.NOP);

            try {
                zkConnection.createOrReset(zkNodePath, "hi", CreateMode.PERSISTENT);
                zkConnection.createOrReset(zkAdminPath, adminMessage.serialize(), CreateMode.PERSISTENT);
            } catch (KeeperException | InterruptedException e) {
                logger.error("Failed to create KVServer and admin ZNodes for node " + node.getNodeName(), e);
                continue;
            }

            nodes.add(node);
        }

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

    private boolean moveDataBetweenNodes(ECSNode fromNode, ECSNode toNode) {
        boolean success = true;

        try {
            AdminMessage message = new AdminMessage(AdminMessage.Action.RECEIVE_DATA);
            AdminMessage response = zkConnection.sendAdminMessage(toNode.getNodeName(), message, 10000);

            if (response.getAction() == AdminMessage.Action.ACK) {
                logger.info("Ready to receive data for server " + toNode.getNodeName());
                int port = Integer.parseInt(response.getMessage());
                success = nodeMoveData(fromNode, toNode, port);
            } else {
                logger.error("Could not receive data at server " + toNode.getNodeName());
                success = false;
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error("Failed to send admin message to receive data at " + toNode.getNodeName(), e);
            success = false;
        } catch (TimeoutException e) {
            logger.error("Timeout while trying to send admin message to receive data at " + toNode.getNodeName());
            success = false;
        }

        return success;
    }

    private boolean nodeMoveData(ECSNode fromNode, ECSNode toNode, int port){
        boolean success = true;

        AdminMessage message = new AdminMessage(AdminMessage.Action.MOVE_DATA);
        message.setSender(fromNode);
        message.setReceiver(toNode);
        message.setMessage(String.valueOf(port));

        try {
            AdminMessage response = zkConnection.sendAdminMessage(fromNode.getNodeName(), message, 10000);

            if (response.getAction() == AdminMessage.Action.ACK) {
                logger.info("Sending data from server " + toNode.getNodeName());
            } else {
                logger.error("Could not send data from server " + toNode.getNodeName());
                success = false;
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error("Failed to send admin message to send data at " + toNode.getNodeName(), e);
            success = false;
        } catch (TimeoutException e) {
            logger.error("Timeout while trying to send admin message to send data at " + toNode.getNodeName());
            success = false;
        }

        return success;
    }

    /**
     * Set or unset the write lock for the given node
     *
     * @param node The node to set the status on
     * @param lock True to set the lock, false to unset
     * @return success status
     */
    private boolean setWriteLock(ECSNode node, boolean lock) {
        boolean success = true;

        AdminMessage message = new AdminMessage(AdminMessage.Action.WRITE_LOCK);

        try {
            AdminMessage response = zkConnection.sendAdminMessage(node.getNodeName(), message, 10000);

            if (response.getAction() == AdminMessage.Action.ACK) {
                logger.info("Write lock " + (lock ? "set" : "unlocked") + " on node " + node.getNodeName());
            } else {
                logger.error("Failed to " + (lock ? "set" : "unlock") + " write lock on node " + node.getNodeName());
                success = false;
            }

        } catch (KeeperException | InterruptedException e) {
            logger.error("Failed to send admin message to " + (lock ? "set" : "unlock") + " write lock for " + node.getNodeName(), e);
            success = false;
        } catch (TimeoutException e) {
            logger.error("Timeout while trying to send admin message to " + (lock ? "set" : "unlock") + " write lock for " + node.getNodeName());
            success = false;
        }

        return success;
    }


}
