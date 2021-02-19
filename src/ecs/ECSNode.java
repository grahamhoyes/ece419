package ecs;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ECSNode implements IECSNode, Comparable<ECSNode> {
    public static BigInteger HASH_MAX = new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", 16);
    private final String hostname;
    private final int port;
    private final String nodeHash;
    private String predecessorHash;

    public ECSNode(String hostname, int port) {
        this.hostname = hostname;
        this.port = port;

        this.nodeHash = md5Hash(getNodeName());
    }

    public String getNodeName() {
        return hostname + ":" + port;
    }

    public String getNodeHost() {
        return hostname;
    }

    public int getNodePort() {
        return port;
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

        if (predecessorHash == null) {
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

    public void setPredecessor(String predecessorHash) {
        this.predecessorHash = predecessorHash;
    }

    @Override
    public int compareTo(ECSNode other) {
        return this.getNodeHash().compareTo(other.getNodeHash());
    }
}
