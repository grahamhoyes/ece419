package ecs;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import shared.messages.AdminMessage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ZooKeeperConnection {
    public static final Logger logger = Logger.getLogger("ZooKeeperConnection");

    public static String ZK_SERVER_ROOT = "/servers";
    public static String ZK_HEARTBEAT_ROOT = "/heartbeats";
    private ZooKeeper zk;

    public ZooKeeper connect(String host, int port) throws InterruptedException, IOException {
        CountDownLatch connectionLatch = new CountDownLatch(1);

        zk = new ZooKeeper(host, port, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectionLatch.countDown();
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

    /**
     * Send an admin message to a given node, and synchronously return the response.
     *
     * Assumes the admin node already exists
     *
     * @return AdminMessage response
     */
    public AdminMessage sendAdminMessage(String nodeName, AdminMessage message, int timeoutMillis) throws KeeperException, InterruptedException, TimeoutException {

        String uuid = UUID.randomUUID().toString();
        message.setUuid(uuid);

        String nodePath = ZK_SERVER_ROOT + "/" + nodeName;
        String adminPath = nodePath + "/admin";
        String responsePath = nodePath + "/" + uuid;

        // Create a node for the response
        try {
            create(responsePath, "", CreateMode.EPHEMERAL, 5);
        } catch (KeeperException | InterruptedException e) {
            logger.error("Failed to create response ZNode", e);
            throw e;
        }

        // Server will respond by setting an ack or error status on its ZNode
        CountDownLatch sig = new CountDownLatch(1);

        // Don't ask me why this has to be an array, IntelliJ says so
        final AdminMessage[] response = new AdminMessage[1];

        // Setup the response watcher first, just to prevent any race conditions
        zk.exists(responsePath, event -> {
            try {
                byte[] data = zk.getData(responsePath, false, null);
                response[0] = new AdminMessage(new String(data));
                sig.countDown();
            } catch (KeeperException | InterruptedException e) {
                logger.error("Failed to receive admin message", e);
            }
        });

        // Send the message
        setData(adminPath, message.serialize());

        if (timeoutMillis > 0) {
            boolean success = sig.await(timeoutMillis, TimeUnit.MILLISECONDS);

            if (!success) {
                // Most of the time, if we timed out it was because the watcher above missed the
                // message. This tries to retrieved it anyway

                try {
                    byte[] data = zk.getData(responsePath, false, null);
                    String responseText = new String(data);

                    if (responseText.equals("")) {
                        logger.error("Node " + nodeName + " did not properly respond to the admin message");
                        throw new TimeoutException("Timeout waiting on response to admin message to node " + nodeName);
                    }
                    response[0] = new AdminMessage(new String(data));

                    if (response[0].getUuid().equals(uuid)) {
                        return response[0];
                    } else {
                        logger.warn("Retrieved UUID " + response[0].getUuid() + " does not match expected UUID " + uuid);
                    }

                } catch (KeeperException | InterruptedException e) {
                    logger.error("Failed to receive admin message", e);
                }

                throw new TimeoutException("Timeout waiting on response to admin message to node " + nodeName);
            }
        } else {
            sig.await();
        }

        return response[0];
    }

    public void close() throws InterruptedException {
        zk.close();
    }
}
