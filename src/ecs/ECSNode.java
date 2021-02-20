package ecs;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ECSNode implements IECSNode, Comparable<ECSNode> {
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
     * A node is responsible for all hash values between it (inclusive)
     * and its predecessor (exclusive). A check of if the hash of a key
     * falls in this node would look something like:
     *   if (hashRange[0] < hash(key) <= hashRange[1]) { ... }
     * Except this is Java, so we can't do something scandalous like compare
     * strings with < or <= operators
     *
     * @return Array of two strings representing the lower bound / predecessor (**exclusive**)
     *         and upper bound / this node hash (**inclusive**) that this node is responsible for
     */
    public String[] getNodeHashRange() {
        if (predecessorHash == null) {
            return null;
        }

        return new String[] { predecessorHash, this.getNodeHash() };
    }

    public boolean isNodeResponsible(String key) {
        String keyHash = md5Hash(key);

        if (predecessorHash == null) {
            // This is the only server
            return true;
        }

        String thisHash = this.getNodeHash();

        // Handle the special case where we loop over the ring boundary
        if (predecessorHash.compareTo(thisHash) < 0) {
            return predecessorHash.compareTo(keyHash) < 0 || keyHash.compareTo(thisHash) <= 0;
        } else {
            return predecessorHash.compareTo(keyHash) < 0 && keyHash.compareTo(thisHash) <= 0;
        }
    }

    public static boolean hashInRange(String hash, String[] hashRange){
        String lower = hashRange[0];
        String upper = hashRange[1];

        if (upper.compareTo(lower) <= 0) {
            return lower.compareTo(hash) <= 0 || upper.compareTo(hash) >= 0;
        } else {
            return lower.compareTo(hash) <= 0 && upper.compareTo(hash) >= 0;
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
