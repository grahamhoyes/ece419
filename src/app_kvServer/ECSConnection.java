package app_kvServer;

import ecs.HashRing;
import ecs.ZooKeeperConnection;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

public class ECSConnection {
    private static final Logger logger = Logger.getRootLogger();

    private final KVServer kvServer;
    private ZooKeeperConnection zkConnection;
    private ZooKeeper zk;
    private String serverName;
    private final String nodePath;
    private HashRing hashRing;

    // TODO: Make this a watcher

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

        // Create a ZNode for this instance. Because we use child nodes, the node cannot
        // be persistent.
        // TODO: Delete this node when the server is stopped
        //  How will the ECS know if this server disconnects now?

        try {
            Stat stat = zk.exists(this.nodePath, false);

            if (stat == null) {
                zkConnection.create(this.nodePath, "hi", CreateMode.PERSISTENT);
            } else {
                // Delete all children and reset the node
                List<String> children = zk.getChildren(this.nodePath, false, null);
                for (String child : children) {
                    zk.delete(child, -1);
                }
                zkConnection.setData(this.nodePath, "hi");
            }

        } catch (KeeperException | InterruptedException e) {
            logger.fatal("Unable to create ZNode");
            e.printStackTrace();
            System.exit(1);
        }

        logger.info("ZNode crated at " + nodePath);

        // Fetch the metadata from zookeeper
        try {
            byte[] metadata = zk.getData(ZooKeeperConnection.ZK_METADATA_PATH, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    // TODO: if (!running) return;
                    try {
                        byte[] metadata = zk.getData(ZooKeeperConnection.ZK_METADATA_PATH, this, null);
                        hashRing = new HashRing(new String(metadata));
                    } catch (InterruptedException | KeeperException e) {
                        logger.error("Failed to fetch matadata");
                    }
                }
            }, null);

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
     * Watcher for admin messages coming over ZooKeeper.
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

                // TODO: Admin messaging format
            } catch (KeeperException | InterruptedException e) {
                logger.warn("Unable to process Admin watch event");
                e.printStackTrace();
            }
        }
    }
}
