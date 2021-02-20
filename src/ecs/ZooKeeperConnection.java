package ecs;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

public class ZooKeeperConnection {
    public static final Logger logger = Logger.getRootLogger();

    public static String ZK_SERVER_ROOT = "/servers";
    public static String ZK_HEARTBEAT_ROOT = "/heartbeats";
    public static String ZK_METADATA_PATH = ZK_SERVER_ROOT + "/" + "metadata";
    CountDownLatch connectionLatch;
    private ZooKeeper zk;

    public ZooKeeper connect(String host, int port) throws InterruptedException, IOException {
        connectionLatch = new CountDownLatch(1);

        zk = new ZooKeeper(host, port, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    connectionLatch.countDown();
                }
            }
        });

        connectionLatch.await();
        return zk;
    }

    public void create(String path, String data, CreateMode mode) throws KeeperException, InterruptedException {
        zk.create(path, data.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
    }

    public void create(String path, String data, CreateMode mode, int retry) throws KeeperException, InterruptedException {
        for (int i = 0; i < retry; i++) {
            try {
                zk.create(path, data.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
                return;
            } catch (KeeperException e) {
                if (i == retry - 1) {
                    throw e;
                }

                try {
                    logger.info("Failed to create node, retrying " + (retry - i - 1) + " more times...");
                    Thread.sleep(5000);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    public void setData(String path, String data) throws KeeperException, InterruptedException {
        zk.setData(path, data.getBytes(StandardCharsets.UTF_8), -1);
    }

    public void createOrReset(String path, String data, CreateMode mode) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(path, false);

        if (stat == null) {
            create(path, data, mode);
        } else {
            setData(path, data);
        }
    }

    public void delete(String path) throws KeeperException, InterruptedException {
        zk.delete(path, -1);
    }

    public void close() throws InterruptedException {
        zk.close();
    }
}
