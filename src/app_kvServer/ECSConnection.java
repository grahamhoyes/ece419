package app_kvServer;

import ecs.ECSNode;
import ecs.HashRing;
import ecs.ZooKeeperConnection;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import shared.messages.AdminMessage;

import java.io.IOException;
import java.util.List;

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
            logger.fatal("Unable to check for ECS root node");
            e.printStackTrace();
            System.exit(1);
        }

        try {
            // Create a ZNode for this instance. Because we use child nodes, the node cannot
            // be persistent.
            Stat stat = zk.exists(this.nodePath, false);

            if (stat == null) {
                zkConnection.create(this.nodePath, "STARTING", CreateMode.PERSISTENT);
            } else {
                // Delete all children and reset the node
                List<String> children = zk.getChildren(this.nodePath, false, null);
                for (String child : children) {
                    zkConnection.delete(this.nodePath + "/" + child);
                }
                zkConnection.setData(this.nodePath, "STARTING");
            }

            // Create an ephemeral heartbeat node. This is used by the ECS to detect
            // disconnects
            String heartbeatPath = ZooKeeperConnection.ZK_HEARTBEAT_ROOT + "/" + this.serverName;
            zkConnection.create(heartbeatPath, "heartbeat", CreateMode.EPHEMERAL);

        } catch (KeeperException | InterruptedException e) {
            logger.fatal("Unable to create ZNode");
            e.printStackTrace();
            System.exit(1);
        }

        logger.info("ZNode crated at " + nodePath);

        // Fetch the metadata from zookeeper
        try {
            byte[] metadata = zk.getData(ZooKeeperConnection.ZK_METADATA_PATH, new MetadataWatcher(), null);
            hashRing = new HashRing(new String(metadata));
        } catch (InterruptedException | KeeperException e) {
            logger.fatal("Failed to fetch matadata");
            e.printStackTrace();
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
    }

    /**
     * Watcher for metadata changes coming over ZooKeeper
     *
     * Updates the local metadata store (hashRing)
     * // TODO: is there anything else to do when metadata changes?
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
            } catch (InterruptedException | KeeperException e) {
                logger.error("Failed to fetch metadata");
                e.printStackTrace();
            }
        }
    }

    /**
     * Watcher for admin messages coming over ZooKeeper
     *
     * Admin messages come to /servers/<server>/admin
     *
     * After processing the message, the node should be deleted.
     * TODO: How should the KVServer inform the ECS of errors?
     */
    private class AdminWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            if (!kvServer.isRunning()) return;

            String adminPath = nodePath + "/admin";

            System.out.println("Watcher triggered");

            try {
                byte[] data = zk.getData(adminPath, false, null);
                AdminMessage message = new AdminMessage(new String(data));

                AdminMessage response = new AdminMessage();
                boolean success = true;

                switch (message.getAction()) {
                    case INIT:
                        kvServer.setStatus(IKVServer.ServerStatus.STOPPED);
                        nodeMetadata = message.getNodeMetadata();

                        // At this point, the node is not aware of the metadata of any other
                        // nodes in the ring, and they are not aware that this node has been
                        // added. Global metadata updates are caught by  MetadataWatcher

                        break;
                    case START:
                        break;
                    case STOP:
                        break;
                    case SHUT_DOWN:
                        break;
                    case WRITE_LOCK:
                        break;
                    case WRITE_UNLOCK:
                        break;
                    case MOVE_DATA:
                        break;
                    case RECEIVE_DATA:
                        break;
                    case SET_METADATA:
                        // Sets only the current node's metadata, without updating
                        // the hash ring. MetadataWatcher handles that
                        nodeMetadata = message.getNodeMetadata();
                        break;
                    default:
                        success = false;
                        response.setMessage("Invalid action");
                }

                if (success) {
                    response.setAction(AdminMessage.Action.ACK);
                } else {
                    // Messages is expected to be set above
                    response.setAction(AdminMessage.Action.ERROR);
                }

                // Send the response back on the same ZNode, which the ECS
                // listens to with a watcher
                zkConnection.setData(adminPath, response.serialize());

                // Re-register the watch so it can be triggered again
                zk.exists(adminPath, this);
            } catch (KeeperException | InterruptedException e) {
                logger.warn("Unable to process Admin watch event");
                e.printStackTrace();
            }
        }
    }
}
