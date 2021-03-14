package ecs;

import app_kvServer.IKVServer;
import org.apache.log4j.Logger;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ServerNode implements IECSNode, Comparable<ServerNode> {
    public static final Logger logger = Logger.getLogger("ServerNode");
    public static BigInteger HASH_MAX = new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16);
    private final String name;
    private final String hostname;
    private final int port;
    private final String nodeHash;
    private String predecessorHash;
    IKVServer.ServerStatus status;

    public ServerNode(String name, String hostname, int port) {
        this.name = name;
        this.hostname = hostname;
        this.port = port;
        this.status = IKVServer.ServerStatus.OFFLINE;

        this.nodeHash = md5Hash(getNodeName());
    }

    public String getNodeName() {
        return this.name;
    }

    public String getNodeHost() {
        return hostname;
    }

    public int getNodePort() {
        return port;
    }

    public String getPredecessorHash() {
        return this.predecessorHash;
    }

    public IKVServer.ServerStatus getStatus() {
        return status;
    }

    public void setPredecessor(String predecessorHash) {
        this.predecessorHash = predecessorHash;
    }

    public void setStatus(IKVServer.ServerStatus status) {
        this.status = status;
    }


    public static String md5Hash(String data) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.update(data.getBytes());
            byte[] digest = md5.digest();

            // Convert the digest into a string of hex characters
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(Integer.toHexString((b & 0xFF) | 0x100), 1, 3);
            }
            return sb.toString();

        } catch (NoSuchAlgorithmException e) {
            // This will never happen since MD5 is hard-coded, but welcome to Java
            return "00000000000000000000000000000000";
        }
    }

    public String getNodeHash() {
        return this.nodeHash;
    }

    /**
     * Return an array of hashes this node is responsible for.
     *
     * @return Array of two strings representing the lower bound and upper bound hashes
     * that this node is responsible for, both inclusive
     */
    public String[] getNodeHashRange() {
        if (predecessorHash == null) {
            return null;
        }

        BigInteger startingHashValue = new BigInteger(predecessorHash, 16)
                .add(BigInteger.ONE)
                .mod(HASH_MAX);

        StringBuilder startingHash = new StringBuilder(startingHashValue.toString(16));

        while (startingHash.length() < 32) {
            startingHash.insert(0, "0");
        }


        return new String[] { startingHash.toString(), this.getNodeHash() };
    }

    public boolean isNodeResponsible(String key) {
        String keyHash = md5Hash(key);

        if (predecessorHash == null || predecessorHash.equals(this.getNodeHash())) {
            // This is the only server
            return true;
        }

        String thisHash = this.getNodeHash();

        if (predecessorHash.compareTo(thisHash) > 0) {  // TODO: Is this correct?
            // Handle the special case where we loop over the ring boundary
            return predecessorHash.compareTo(keyHash) < 0 || keyHash.compareTo(thisHash) <= 0;
        } else {
            return predecessorHash.compareTo(keyHash) < 0 && keyHash.compareTo(thisHash) <= 0;
        }
    }

    /**
     * Check if the given hash falls within the hash range
     *
     * @param hash Hash value to check
     * @param hashRange List of [lower bound, upper bound] hashes, inclusive
     * @return True if the hash is in the range, false otherwise
     */
    public static boolean hashInRange(String hash, String[] hashRange) {
        if (hashRange == null) return false;

        String lower = hashRange[0];
        String upper = hashRange[1];

        if (upper.compareTo(lower) <= 0) {
            return lower.compareTo(hash) <= 0 || upper.compareTo(hash) >= 0;
        } else {
            return lower.compareTo(hash) <= 0 && upper.compareTo(hash) >= 0;
        }
    }


    @Override
    public int compareTo(ServerNode other) {
        return this.getNodeHash().compareTo(other.getNodeHash());
    }

    /**
     * @return A deep copy of this ServerNode
     */
    public ServerNode copy() {
        ServerNode copyNode = new ServerNode(name, hostname, port);
        copyNode.predecessorHash = this.predecessorHash;
        copyNode.status = this.status;

        return copyNode;
    }
}
