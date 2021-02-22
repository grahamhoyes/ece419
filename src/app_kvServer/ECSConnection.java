package app_kvServer;

import ecs.ECSNode;
import ecs.HashRing;
import ecs.ZooKeeperConnection;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import shared.messages.AdminMessage;

import java.io.IOException;
import java.util.Arrays;

public class ECSConnection {
    private static final Logger logger = Logger.getRootLogger();

    private final KVServer kvServer;
    private final ZooKeeperConnection zkConnection;
    private ZooKeeper zk;
    private final String serverName;
    private final String nodePath;
    private ECSNode nodeMetadata;  // Metadata for this node only
    private HashRing hashRing;     // Metadata for all nodes

    public ECSConnection(String host, int port, String serverName, KVServer kvServer) {
        this.kvServer = kvServer;
        this.serverName = serverName;
        this.nodePath = ZooKeeperConnection.ZK_SERVER_ROOT + "/" + serverName;

        zkConnection = new ZooKeeperConnection();

        try {
            zk = zkConnection.connect(host, port);
        } catch (InterruptedException | IOException e) {
            logger.fatal("Failed to establish a connection to ZooKeeper");
            e.printStackTrace();
            System.exit(1);
        }

        // Check that the ZooKeeper servers root node has been setup
        try {
            Stat stat = zk.exists(ZooKeeperConnection.ZK_SERVER_ROOT, false);

            if (stat == null) {
                logger.error("ZooKeeper has not been initialized");
                System.exit(1);
            }

        } catch (KeeperException | InterruptedException e) {
            logger.fatal("Unable to check for ECS root node", e);
            System.exit(1);
        }

        // Check that the ECS has initialized the server and admin ZNodes
        try {
            if ((zk.exists(this.nodePath, false)) == null) {
                logger.fatal("Server ZNode has not been created");
                System.exit(1);
            }

            if ((zk.exists(this.nodePath + "/admin", false)) == null ) {
                logger.fatal("Admin ZNode has not been created");
                System.exit(1);
            }
        } catch (KeeperException | InterruptedException e) {
            logger.fatal("Unable to check for ECS Znodes", e);
            System.exit(1);
        }

        // Set a watcher on the admin node
        try {
            zk.exists(this.nodePath + "/admin", new AdminWatcher());
        } catch (InterruptedException | KeeperException e) {
            logger.fatal("Failed to add watcher for admin messages");
            e.printStackTrace();
            System.exit(1);
        }

        // Fetch the metadata from zookeeper and set the watcher
        // On first initialization, this will be incorrect. The node will be informed of
        // its own metadata by an admin message, and after all nodes have been added
        // global metadata will be broadcasted and updated by the watcher.
        try {
            byte[] metadata = zk.getData(ZooKeeperConnection.ZK_METADATA_PATH, new MetadataWatcher(), null);
            hashRing = new HashRing(new String(metadata));
        } catch (InterruptedException | KeeperException e) {
            logger.fatal("Failed to fetch matadata");
            e.printStackTrace();
            System.exit(1);
        }

        try {
            // Create an ephemeral heartbeat node. This is used by the ECS to detect
            // disconnects, and to indicate that the node is up
            String heartbeatPath = ZooKeeperConnection.ZK_HEARTBEAT_ROOT + "/" + this.serverName;
            zkConnection.create(heartbeatPath, "heartbeat", CreateMode.EPHEMERAL, 7);
        } catch (KeeperException | InterruptedException e) {
            logger.fatal("Unable to create heartbeat ZNode", e);
            System.exit(1);
        }

        logger.info("ZNode crated at " + nodePath);

    }

    /**
     * Watcher for metadata changes for the entire hash ring coming over ZooKeeper
     *
     * Updates the local metadata store (hashRing)
     * // TODO: is there anything else to do when metadata changes?
     * // TODO: This probably isn't getting used at all, since we need it synchronous
     *
     * Metadata is at /servers/metadata
     */
    private class MetadataWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            if (!kvServer.isRunning()) return;

            try {
                byte[] metadata = zk.getData(ZooKeeperConnection.ZK_METADATA_PATH, this, null);
                hashRing = new HashRing(new String(metadata));

                // Update the metadata for this node as well
                nodeMetadata = hashRing.getNode(serverName);
            } catch (InterruptedException | KeeperException e) {
                logger.error("Failed to fetch metadata", e);
            }
        }
    }

    /**
     * Check if this node is responsible for the given key
     */
    public boolean isNodeResponsible(String key) {
        return this.nodeMetadata.isNodeResponsible(key);
    }

    /**
     * Get the hash ring
     */
    public HashRing getHashRing() {
        return this.hashRing;
    }

    /**
     * Watcher for admin messages coming over ZooKeeper
     *
     * Admin messages come to /servers/<server>/admin
     *
     * After processing the message, the node should be deleted.
     */
    private class AdminWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
             // TODO: Should this be here?
//            if (!kvServer.isRunning()) return;

            String adminPath = nodePath + "/admin";

            try {
                byte[] data = zk.getData(adminPath, false, null);
                AdminMessage message = new AdminMessage(new String(data));

                AdminMessage response = new AdminMessage();

                logger.info("Got a " + message.getAction() + " admin message");

                switch (message.getAction()) {
                    case NOP:
                        break;
                    case INIT:
                        kvServer.setStatus(IKVServer.ServerStatus.STOPPED);
                        // At this point, the node is not aware of the metadata of any other
                        // nodes in the ring, and they are not aware that this node has been
                        // added. Global metadata updates are caught by MetadataWatcher

                        response.setAction(AdminMessage.Action.ACK);
                        logger.info("Server initialized");
                        break;
                    case START:
                        kvServer.start();
                        response.setAction(AdminMessage.Action.ACK);
                        break;
                    case STOP:
                        kvServer.stop();
                        response.setAction(AdminMessage.Action.ACK);
                        break;
                    case SHUT_DOWN:
                        break;
                    case WRITE_LOCK:
                        kvServer.lockWrite();
                        response.setAction(AdminMessage.Action.ACK);
                        break;
                    case WRITE_UNLOCK:
                        kvServer.unlockWrite();
                        response.setAction(AdminMessage.Action.ACK);
                        break;
                    case MOVE_DATA:
                        kvServer.sendData(message);
                        response.setAction(AdminMessage.Action.ACK);
                        break;
                    case RECEIVE_DATA:
                        int port = kvServer.setupDataReceiver();
                        response.setAction(AdminMessage.Action.ACK);
                        response.setMessage(String.valueOf(port));
                        break;
                    case CLEANUP_DATA:
                        kvServer.cleanUpData();
                        response.setAction(AdminMessage.Action.ACK);
                        break;
                    case SET_METADATA:
                        // TODO: Is it fine to update the entire hash ring here? Probably
                        hashRing = message.getMetadata();
                        nodeMetadata = hashRing.getNode(serverName);
                        response.setAction(AdminMessage.Action.ACK);
                        logger.info("Metadata updated");
                        logger.info("Server now responsible for " + Arrays.toString(nodeMetadata.getNodeHashRange()));
                        break;
                    default:
                        response.setAction(AdminMessage.Action.ERROR);
                        response.setMessage("Invalid action");
                }

                // Send the response back at the node's ZNode, which the ECS
                // has a watcher for
                zkConnection.setData(nodePath, response.serialize());

                // Re-register the watch so it can be triggered again
                zk.exists(adminPath, this);
            } catch (KeeperException | InterruptedException e) {
                logger.warn("Unable to process Admin watch event");
                e.printStackTrace();
            }
        }
    }
}
