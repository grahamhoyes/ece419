package ecs;

public class ServerSettings {
    private boolean replicatorsExpireKeys;

    public ServerSettings() {
        replicatorsExpireKeys = false;
    }

    public void setReplicatorsExpireKeys(boolean replicatorsExpireKeys) {
        this.replicatorsExpireKeys = replicatorsExpireKeys;
    }

    public boolean getReplicatorsExpireKeys() {
        return this.replicatorsExpireKeys;
    }
}
