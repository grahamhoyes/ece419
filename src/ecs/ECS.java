package ecs;

import app_kvServer.IKVServer;
import app_kvServer.KVServer;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import shared.messages.AdminMessage;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.*;

public class ECS implements IECS {
    private static final Logger logger = Logger.getLogger("ECS");
    private boolean DEBUG;

    // ZooKeeper is assumed to be running on the default port
    // on this machine
    private final String zkHost;
    private final int zkPort;

    private static final String cacheStrategy = "";
    private static final int cacheSize = 0;

    String remotePath;
    private final ZooKeeperConnection zkConnection;
    private ZooKeeper zk;

    // Map server name to ServerNode
    private HashMap<String, ServerNode> configMap = new HashMap<>();

    // Queue of inactive ServerNode
    private Queue<ServerNode> nodePool = new LinkedList<>();

    // Hash ring of active nodes
    private HashRing hashRing;

    // Set of active nodes, updated faster than the hash ring
    private Set<String> activeNodeSet = new HashSet<>();

    // Queue used to update global metadata asynchronously. This is utterly disgusting,
    // but we can't set the watcher used by updateGlobalMetadata() within another watcher,
    // so it is somewhat necessary
    private BlockingQueue<Boolean> doAsynchronousMetadataUpdate = new ArrayBlockingQueue<>(10);

    public ECS(String configFileName, String zkHost, int zkPort, String remotePath) {
        this.remotePath = remotePath;
        this.zkHost = zkHost;
        this.zkPort = zkPort;

        String debug_env = System.getenv().getOrDefault("DEBUG", "0");
        this.DEBUG = !debug_env.equals("0");

        if (this.DEBUG) {
            logger.info("== ECS running in Debug mode ==");
        }


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

                ServerNode node = new ServerNode(name, host, Integer.parseInt(port));
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
            zk = zkConnection.connect(this.zkHost, this.zkPort);
        } catch (InterruptedException | IOException e) {
            logger.fatal("Failed to establish a connection to ZooKeeper");
            e.printStackTrace();
            System.exit(1);
        }

        // Create the server root and heartbeat nodes if they don't exist
        try {
            zkConnection.createOrReset(ZooKeeperConnection.ZK_SERVER_ROOT, "root", CreateMode.PERSISTENT);
            zkConnection.createOrReset(ZooKeeperConnection.ZK_HEARTBEAT_ROOT, "heartbeat", CreateMode.PERSISTENT);
        } catch (InterruptedException | KeeperException e) {
            logger.fatal("Unable to create ZNode", e);
            System.exit(1);
        }

        new Thread(new MetadataUpdater()).start();
    }

    @Override
    public boolean start() {
        boolean startedAll = true;
        for (ServerNode node : hashRing.getNodes()) {
            node.setStatus(IKVServer.ServerStatus.STOPPED);
            AdminMessage message = new AdminMessage(AdminMessage.Action.START);
            try {
                AdminMessage response = zkConnection.sendAdminMessage(node.getNodeName(), message, 20000);
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

        try {
            updateGlobalMetadata();
        } catch (KeeperException | InterruptedException | TimeoutException e) {
            logger.error("Failed to update global metadata stopping servers", e);
        }

        return startedAll;
    }

    @Override
    public boolean stop() {
        boolean stoppedAll = true;
        for (ServerNode node : hashRing.getNodes()) {
            node.setStatus(IKVServer.ServerStatus.STOPPED);
            AdminMessage message = new AdminMessage(AdminMessage.Action.STOP);
            try {
                AdminMessage response = zkConnection.sendAdminMessage(node.getNodeName(), message, 20000);
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

        try {
            updateGlobalMetadata();
        } catch (KeeperException | InterruptedException | TimeoutException e) {
            logger.error("Failed to update global metadata stopping servers", e);
        }

        return stoppedAll;
    }

    @Override
    public boolean shutdown() {
        boolean success = true;
        for (ServerNode node : hashRing.copy().getNodes()) {
            hashRing.removeNode(node.getNodeName());
            success = success & shutdownNode(node);
        }
        return success;
    }

    @Override
    public ServerNode addNode() {
        return addNode(cacheStrategy, cacheSize);
    }

    @Override
    public ServerNode addNode(String cacheStrategy, int cacheSize) {
        List<ServerNode> nodes = (List<ServerNode>) setupNodes(1, cacheStrategy, cacheSize);
        if (nodes == null) return null;

        ServerNode node = nodes.get(0);

        HashRing updatedHashRing = hashRing.copy();
        updatedHashRing.addNode(node);

        // Initialize the server
        AdminMessage adminMessage = new AdminMessage(AdminMessage.Action.INIT);

        try {
            AdminMessage response = zkConnection.sendAdminMessage(
                    node.getNodeName(), adminMessage, 20000
            );

            if (response.getAction() == AdminMessage.Action.ACK) {
                ServerNode receiver = response.getReceiver();
                node.setDataReceivePort(receiver.getDataReceivePort());
                node.setReplicationReceivePort(receiver.getReplicationReceivePort());
                logger.info("Server " + node.getNodeName() + " initialized");
            } else {
                logger.error("Server " + node.getNodeName() + " failed to initialize");
                logger.debug(response.getMessage());
                return null;
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error("Failed to send admin message to initialize " + node.getNodeName(), e);
            return null;
        } catch (TimeoutException e) {
            logger.error("Timeout while trying to send admin message to initialize " + node.getNodeName());
            return null;
        }

        // Set the metadata for the added server
        adminMessage.setAction(AdminMessage.Action.SET_METADATA);
        adminMessage.setMetadata(updatedHashRing);

        try {
            AdminMessage response = zkConnection.sendAdminMessage(node.getNodeName(), adminMessage, 20000);

            if (response.getAction() == AdminMessage.Action.ACK) {
                logger.info("Set metadata for server " + node.getNodeName());
            } else {
                logger.error("Could not set metadata for server " + node.getNodeName());
                return null;
            }

        } catch (KeeperException | InterruptedException e) {
            logger.error("Failed to send admin message to set metadata for " + node.getNodeName(), e);
            return null;
        } catch (TimeoutException e) {
            logger.error("Timeout while trying to send admin message to set metadata for " + node.getNodeName());
            return null;
        }

        // Start the server
        adminMessage.setAction(AdminMessage.Action.START);
        node.setStatus(IKVServer.ServerStatus.ACTIVE);
        adminMessage.setMetadata(null);

        try {
            AdminMessage response = zkConnection.sendAdminMessage(node.getNodeName(), adminMessage, 20000);

            if (response.getAction() == AdminMessage.Action.ACK) {
                logger.info("Started server " + node.getNodeName());
            } else {
                logger.error("Could not start server " + node.getNodeName());
                return null;
            }

        } catch (KeeperException | InterruptedException e) {
            logger.error("Failed to send admin message to start server " + node.getNodeName(), e);
            return null;
        } catch (TimeoutException e) {
            logger.error("Timeout while trying to send admin message to start server " + node.getNodeName());
            return null;
        }

        ServerNode successor = node.getSuccessor();
        assert successor != null;

        if (updatedHashRing.getNodes().size() > 1) {
            // Invoke data transfer to its successor
            assert !successor.getNodeName().equals(node.getNodeName());

            if (!setWriteLock(successor, true)) return null;

            // Invoke data transfer
            if (!moveDataBetweenNodes(successor, node)) return null;
        }

        // Once all data has been transferred, send global metadata updates
        hashRing = updatedHashRing;

        try {
            updateGlobalMetadata();
        } catch (KeeperException | InterruptedException | TimeoutException e) {
            logger.error("Failed to update global metadata", e);
            return null;
        }

        if (updatedHashRing.getNodes().size() > 1) {
            // Release the write lock on the successor servers and remove old data
            if (!setWriteLock(successor, false)) return null;

            adminMessage.setAction(AdminMessage.Action.CLEANUP_DATA);

            try {
                AdminMessage response = zkConnection.sendAdminMessage(successor.getNodeName(), adminMessage, 20000);

                if (response.getAction() == AdminMessage.Action.ACK) {
                    logger.info("Data transfer cleanup complete on " + successor.getNodeName());
                } else {
                    logger.error("Could not clean up transfer data on " + successor.getNodeName());
                    // TODO: Rollback
                    return null;
                }

            } catch (KeeperException | InterruptedException e) {
                logger.error("Failed to send admin message to cleanup transfer data on " + successor.getNodeName(), e);
                return null;
            } catch (TimeoutException e) {
                logger.error("Timeout while trying to send admin message to cleanup transfer data on " + successor.getNodeName());
                return null;
            }
        }

        activeNodeSet.add(node.getNodeName());

        logger.info("Successfully launched node " + node.getNodeName());

        return node;
    }

    @Override
    public Collection<ServerNode> addNodes(int count) {
        return addNodes(count, cacheStrategy, cacheSize);
    }

    @Override
    public Collection<ServerNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        List<ServerNode> nodes = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            ServerNode node = addNode(cacheStrategy, cacheSize);
            if (node == null) continue;
            nodes.add(node);
        }

        return nodes;
    }

    @Override
    public Collection<ServerNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        if (count > nodePool.size()) {
            logger.error("Unable to add " + count + " nodes, only " + nodePool.size() + " remaining in the pool");
            return null;
        }

        List<ServerNode> nodes = new ArrayList<>();

        // Get any extra nodes from the offline pool, and start them
        while (count > 0 && nodePool.size() > 0) {
            ServerNode node = nodePool.poll();
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

            String javaCmd = String.join(" ",
                    "java -jar",
                    remotePath,
                    String.valueOf(node.getNodePort()),
                    node.getNodeName(),
                    zkHost,
                    String.valueOf(zkPort));

            boolean isLocal = node.getNodeHost().equals("127.0.0.1") || node.getNodeHost().equals("localhost");

            String cmd;

            if (isLocal) {
                cmd = javaCmd;
            } else {
                cmd = String.join(" ",
                        "ssh -o StrictHostKeyChecking=no -n",
                        node.getNodeHost(),
                        "nohup",
                        javaCmd,
                        "&");
            }

            // Setup the watcher for the server coming online before we launch it
            String zkHeartbeatPath = ZooKeeperConnection.ZK_HEARTBEAT_ROOT + "/" + node.getNodeName();

            // Signal for successful connections, to synchronize otherwise async watchers
            CountDownLatch sig = new CountDownLatch(1);

            Process p = null;
            try {
                // Watcher for heartbeat coming online
                zk.exists(zkHeartbeatPath, event -> sig.countDown());

                try {

                    // Start the server
                    if (DEBUG && isLocal) {
                        KVServer server = new KVServer(node.getNodePort(), node.getNodeName(), zkHost, zkPort);
                        new Thread(server).start();

                        logger.info("New debug KVServer thread started. Logs for server "
                                + node.getNodeName() + " will be combined with the ECS logs.");
                    } else {
                        p = Runtime.getRuntime().exec(cmd);

                        if (isLocal) {
                            logger.info("Local KVServer started with process " + p.pid());
                        } else {
                            logger.info("Remote KVServer started on " + node.getNodeHost() + ":" + node.getNodePort());
                        }
                    }

                } catch (IOException e) {
                    logger.error("Unable to launch node " + node.getNodeName() + " on host " + node.getNodeHost(), e);
                    nodePool.add(node);
                    continue;
                }

                boolean success = sig.await(20000, TimeUnit.MILLISECONDS);

                if (!success) {
                    // Something crashed in the remote process, print its stderr for debugging
                    try {
                        BufferedReader buf = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                        String line;
                        while ((line = buf.readLine()) != null)
                            System.err.println(line);
                    } catch (IOException ignored) {}
                    
                    logger.error("Timeout while waiting to start server " + node.getNodeName());

                    nodePool.add(node);
                    p.destroy();
                    continue;
                }

            } catch (KeeperException | InterruptedException e) {
                logger.error("Error waiting for heartbeat thread for server " + node.getNodeName());
                nodePool.add(node);
                if (p != null) p.destroy();
                continue;
            }

            // Setup the watcher for when the heatbeat node dies
            try {
                zk.exists(zkHeartbeatPath, new HeartbeatDeathWatcher());
            } catch (InterruptedException | KeeperException e) {
                logger.fatal("Failed to set heartbeat watcher for server " + node.getNodeName());
                nodePool.add(node);
                if (p != null) p.destroy();
                continue;
            }

            logger.info("Server " + node.getNodeName() + " has been started");

            nodes.add(node);
            count--;
        }

        return nodes;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        boolean success = true;

        for (String nodeName : nodeNames) {
            success = success && removeNode(nodeName);
        }

        return success;
    }

    public boolean removeNode(String nodeName) {
        ServerNode node = hashRing.getNode(nodeName);
        if (node == null) {
            logger.error("Node " + nodeName + " is not running");
            return false;
        }

        if (hashRing.getNodes().size() == 1) {  // == 0 is captured by node == null as well
            logger.error("Cannot remove last node, would result in permanent data loss");
            return false;
        }

        // Immediately remove the node from the set of active nodes,
        // prior to it being fully removed from the hash ring
        activeNodeSet.remove(nodeName);

        ServerNode successor = node.getSuccessor();

        HashRing updatedHashRing = hashRing.copy();
        updatedHashRing.removeNode(nodeName);

        // Refresh the hash range of the successor
        successor = updatedHashRing.getNode(successor.getNodeName());

        // Write lock the node to be deleted
        if (!setWriteLock(node, true)) return false;

        // Send updated metadata to successor server
        AdminMessage adminMessage = new AdminMessage(AdminMessage.Action.SET_METADATA);
        adminMessage.setMetadata(updatedHashRing);

        try {
            AdminMessage response = zkConnection.sendAdminMessage(successor.getNodeName(), adminMessage, 20000);

            if (response.getAction() == AdminMessage.Action.ACK) {
                logger.info("Set metadata for server " + successor.getNodeName());
            } else {
                logger.error("Could not set metadata for server " + successor.getNodeName());
                return false;
            }

        } catch (KeeperException | InterruptedException e) {
            logger.error("Failed to send admin message to set metadata for " + successor.getNodeName(), e);
            return false;
        } catch (TimeoutException e) {
            logger.error("Timeout while trying to send admin message to set metadata for " + successor.getNodeName());
            return false;
        }

        // Transfer data from the node to be removed to its successor
        node.setPredecessor(null);  // Also sets the hash range to null
        if (!moveDataBetweenNodes(node, successor)) return false;

        // Once all data has been transferred, send global metadata updates
        hashRing = updatedHashRing;

        try {
            updateGlobalMetadata();
        } catch (KeeperException | InterruptedException | TimeoutException e) {
            logger.error("Failed to update global metadata");
            return false;
        }

        // Clean up the data from the old server (and reset the write lock for good measure)
        if (!setWriteLock(node, false)) return false;

        adminMessage.setAction(AdminMessage.Action.CLEANUP_DATA);

        try {
            AdminMessage response = zkConnection.sendAdminMessage(node.getNodeName(), adminMessage, 20000);

            if (response.getAction() == AdminMessage.Action.ACK) {
                logger.info("Data transfer cleanup complete on " + node.getNodeName());
            } else {
                logger.error("Could not clean up transfer data on " + node.getNodeName());
                return false;
            }

        } catch (KeeperException | InterruptedException e) {
            logger.error("Failed to send admin message to cleanup transfer data on " + node.getNodeName(), e);
            return false;
        } catch (TimeoutException e) {
            logger.error("Timeout while trying to send admin message to cleanup transfer data on " + node.getNodeName());
            return false;
        }

        return shutdownNode(node);

    }

    /**
     * Sends a shutdown command to the given node. The node is assumed to already
     * be removed from the storage service. It will be added back to the offline
     * node pool.
     *
     * @return success
     */
    public boolean shutdownNode(ServerNode node) {
        AdminMessage shutdownMessage = new AdminMessage(AdminMessage.Action.SHUT_DOWN);
        String zkHeartbeatPath = ZooKeeperConnection.ZK_HEARTBEAT_ROOT + "/" + node.getNodeName();

        CountDownLatch sig = new CountDownLatch(1);

        // Setup watcher for the heartbeat to disappear after the shutdown signal is sent
        try {
            zk.exists(zkHeartbeatPath, event -> {
                if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                    logger.info("Shutdown complete on node " + node.getNodeName());
                } else {
                    logger.error("Heartbeat ZNode was not properly deleted for node " + node.getNodeName());
                }
                sig.countDown();
            });
        } catch (KeeperException | InterruptedException e) {
            logger.error("Error waiting for heartbeat thread for server " + node.getNodeName());
            return false;
        }

        try {
            AdminMessage response = zkConnection.sendAdminMessage(node.getNodeName(), shutdownMessage, 20000);

            if (response.getAction() == AdminMessage.Action.ACK) {
                logger.info("Shut down acknowledged on node " + node.getNodeName());
            } else {
                logger.error("Could not shut down node " + node.getNodeName());
                return false;
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error("Failed to send admin message to shutdown " + node.getNodeName(), e);
            return false;
        } catch (TimeoutException e) {
            logger.error("Timeout while trying to send admin message to shutdown " + node.getNodeName());
            return false;
        }

        // Wait for shutdown to complete
        try {
            boolean success = sig.await(20000, TimeUnit.MILLISECONDS);

            // Add the node back to the node pool
            node.setStatus(IKVServer.ServerStatus.OFFLINE);
            nodePool.add(node);

            return success;
        } catch (InterruptedException e) {
            logger.error("Error waiting for heartbeat to disappear for node " + node.getNodeName(), e);
        }

        return false;
    }

    @Override
    public Collection<ServerNode> getNodes() {
        return hashRing.getNodes();
    }

    @Override
    public ServerNode getNodeByKey(String Key) {
        // TODO
        return null;
    }

    private boolean moveDataBetweenNodes(ServerNode fromNode, ServerNode toNode) {
        boolean success;

        try {
            AdminMessage message = new AdminMessage(AdminMessage.Action.RECEIVE_DATA);
            AdminMessage response = zkConnection.sendAdminMessage(toNode.getNodeName(), message, 20000);

            if (response.getAction() == AdminMessage.Action.ACK) {
                logger.info("Ready to receive data at server " + toNode.getNodeName());
                success = nodeMoveData(fromNode, toNode);
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

    private boolean nodeMoveData(ServerNode fromNode, ServerNode toNode){
        boolean success = true;

        AdminMessage message = new AdminMessage(AdminMessage.Action.MOVE_DATA);
        message.setSender(fromNode);
        message.setReceiver(toNode);

        try {
            AdminMessage response = zkConnection.sendAdminMessage(fromNode.getNodeName(), message, -1);

            if (response.getAction() == AdminMessage.Action.ACK) {
                logger.info("Sending data from server " + fromNode.getNodeName());
            } else {
                logger.error("Could not send data from server " + fromNode.getNodeName());
                success = false;
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error("Failed to send admin message to send data at " + fromNode.getNodeName(), e);
            success = false;
        } catch (TimeoutException e) {
            logger.error("Timeout while trying to send admin message to send data at " + fromNode.getNodeName());
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
    private boolean setWriteLock(ServerNode node, boolean lock) {

        AdminMessage message = new AdminMessage(
                lock ? AdminMessage.Action.WRITE_LOCK : AdminMessage.Action.WRITE_UNLOCK
        );

        try {
            AdminMessage response = zkConnection.sendAdminMessage(node.getNodeName(), message, 20000);

            if (response.getAction() == AdminMessage.Action.ACK) {
                logger.info("Write lock " + (lock ? "set" : "unlocked") + " on node " + node.getNodeName());
            } else {
                logger.error("Failed to " + (lock ? "set" : "unlock") + " write lock on node " + node.getNodeName());
                return false;
            }

        } catch (KeeperException | InterruptedException e) {
            logger.error("Failed to send admin message to " + (lock ? "set" : "unlock") + " write lock for " + node.getNodeName(), e);
            return false;
        } catch (TimeoutException e) {
            logger.error("Timeout while trying to send admin message to " + (lock ? "set" : "unlock") + " write lock for " + node.getNodeName());
            return false;
        }

        return true;
    }

    /**
     * Synchronously update the global metadata of the cluster
     *
     */
    private void updateGlobalMetadata() throws KeeperException, InterruptedException, TimeoutException {
        for (ServerNode node : hashRing.getNodes()) {
            AdminMessage message = new AdminMessage(AdminMessage.Action.SET_METADATA);
            message.setMetadata(hashRing);

            AdminMessage response = zkConnection.sendAdminMessage(node.getNodeName(), message, 20000);

            if (response.getAction() != AdminMessage.Action.ACK) {
                logger.error("Failed to update metadata on node " + node.getNodeName());
            }
        }
    }

    /**
     * Update the metadata for all nodes. This is an asynchronous approach,
     * triggered by putting to the `doAsynchronousMetadataUpdate` queue.
     *
     * This is necessary because the watcher set by zkConnection.sendAdminMessage to
     * wait for a response cannot be properly set within the HeartbeatDeathWatcher, below.
     * This is the only place that  this should be used.
     */
    private class MetadataUpdater implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    doAsynchronousMetadataUpdate.take();

                    try {
                        updateGlobalMetadata();
                        logger.info("Global metadata updated");
                    } catch (KeeperException | InterruptedException | TimeoutException e) {
                        logger.error("Failed to update global metadata", e);
                    }
                } catch (InterruptedException e) {
                    logger.error(e);
                }
            }
        }
    }

    /**
     * Watcher to detect when a particular heartbeat node goes down
     */
    private class HeartbeatDeathWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeDeleted) {
                String nodeName = event.getPath().split("/")[2];

                if (!activeNodeSet.contains(nodeName)) return;

                logger.info("Node " + nodeName + " has died");

                hashRing.removeNode(nodeName);
                try {
                    doAsynchronousMetadataUpdate.put(true);
                } catch (InterruptedException e) {
                    logger.error(e);
                }
            }
        }
    }
}
