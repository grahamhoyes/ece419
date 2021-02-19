package ecs;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class ECS implements IECS {
    private static final Logger logger = Logger.getRootLogger();

    // ZooKeeper is assumed to be running on the default port
    // on this machine
    private static final String ZK_HOST = "127.0.0.1";
    private static final int ZK_PORT = 2181;

    private static final String cacheStrategy = "";
    private static final int cacheSize = 0;

    private final ZooKeeperConnection zkConnection;
    private ZooKeeper zk;

    private HashRing hashRing;

    public ECS(String configFileName) {
        // TODO: Read in config file

        hashRing = new HashRing();

        zkConnection = new ZooKeeperConnection();

        try {
            zk = zkConnection.connect(ZK_HOST, ZK_PORT);
        } catch (InterruptedException | IOException e) {
            logger.fatal("Failed to establish a connection to ZooKeeper");
            e.printStackTrace();
            System.exit(1);
        }

        // Create the server root heartbeat, and metadata nodes if they don't exist
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
    public IECSNode addNode() {
        return addNode(cacheStrategy, cacheSize);
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
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

}
