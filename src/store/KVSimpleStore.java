package store;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import ecs.ServerNode;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static ecs.ServerNode.hashInRange;

//TODO: Remove replicated data on death and clear it on startup

public class KVSimpleStore implements KVStore{
    protected static final Logger logger = Logger.getLogger("KVSimpleStore");
    private static final String dataDir = "store_data";

    private enum WRITE_ACTION {PUT, UPDATE, DELETE};

    private String fileName;
    private String serverName;

    private String filePath;
    private String sendPath;
    private String keepPath;
    private String writeLogPath;

    private Set<String> replicatedPaths = new HashSet<String>();

    private final ReentrantReadWriteLock writeLogLock = new ReentrantReadWriteLock();


    public KVSimpleStore(String serverName) throws IOException{
        this.serverName = serverName;
        this.fileName = serverName + "_store.txt";

        this.filePath = dataDir + File.separatorChar + fileName;
        this.sendPath = dataDir + File.separatorChar + "~send" + this.fileName;
        this.keepPath = dataDir + File.separatorChar + "~keep" + this.fileName;
        this.writeLogPath = dataDir + File.separatorChar + "~writeLog" + this.fileName;

        prepareFile();
    }

    @Override
    public String getFileName(){
        return fileName;
    }

    @Override
    public String getStoragePath(){
        return filePath;
    }

    @Override
    public String getDataDir(){
        return dataDir;
    }

    @Override
    public String getWriteLogPath(){
        return writeLogPath;
    }

    private void prepareFile() throws IOException {
        File storageDir = new File(dataDir);
        if (!storageDir.exists()){
            boolean storageDirCreated = storageDir.mkdir();
            if (storageDirCreated) {
                logger.info("Storage directory created.");
            }
        } else {
            logger.info("Storage directory exists.");
        }

        File storageFile = new File(this.filePath);

        boolean storageFileCreated = storageFile.createNewFile();

        if (storageFileCreated){
            logger.info("KVSimpleStore: Storage files created.");
        } else {
            logger.info("KVSimpleStore: Storage file found.");
        }
    }

    private KeyValueLocation find(String filePath, String key) throws Exception {
        KeyValueLocation keyValueLocation = new KeyValueLocation();
        Gson gson = new Gson();

        try(RandomAccessFile storageFile = new RandomAccessFile(filePath, "r");) {
            String str;

            long currPointer = storageFile.getFilePointer();
            long prevPointer = currPointer;
            while ((str = storageFile.readLine()) != null) {
                KeyValue keyValue = gson.fromJson(str, KeyValue.class);
                currPointer = storageFile.getFilePointer();
                String currKey = keyValue.getKey();

                if (currKey.equals(key)) {
                    keyValueLocation = new KeyValueLocation(
                            prevPointer,
                            currPointer,
                            keyValue.getValue()
                    );
                    break;
                }
                prevPointer = currPointer;
            }
        } catch (JsonSyntaxException e){
            throw new DataFormatException();
        }

        return keyValueLocation;
    }

    private void deleteKeyValue(String filePath, KeyValueLocation keyValueLocation) throws IOException {
        Path tempPath = Files.createTempFile(serverName, ".txt");

        try(RandomAccessFile tempRAFile = new RandomAccessFile(tempPath.toString(), "rw");
            RandomAccessFile storageFile = new RandomAccessFile(filePath, "rw");){
            FileChannel fromChannel = storageFile.getChannel();
            FileChannel toChannel = tempRAFile.getChannel();

            fromChannel.transferTo(keyValueLocation.getEndPosition(), fromChannel.size(), toChannel);
            storageFile.setLength(keyValueLocation.getStartPosition());
            fromChannel.position(keyValueLocation.getStartPosition());
            toChannel.transferTo(0, toChannel.size(), fromChannel);

            Files.deleteIfExists(tempPath);
        }
    }

    private void updateKeyValue(String filePath, KeyValueLocation keyValueLocation, String keyValue) throws  IOException {
        Path tempPath = Files.createTempFile(serverName, ".txt");

        try(RandomAccessFile tempRAFile = new RandomAccessFile(tempPath.toString(), "rw");
            RandomAccessFile storageFile = new RandomAccessFile(filePath, "rw");
        ) {
            FileChannel fromChannel = storageFile.getChannel();
            FileChannel toChannel = tempRAFile.getChannel();

            fromChannel.transferTo(keyValueLocation.getEndPosition(), fromChannel.size(), toChannel);
            storageFile.setLength(keyValueLocation.getStartPosition());

            storageFile.seek(keyValueLocation.getStartPosition());
            storageFile.write(keyValue.getBytes());
            fromChannel.position(storageFile.getFilePointer());

            toChannel.transferTo(0, toChannel.size(), fromChannel);
        }
        Files.deleteIfExists(tempPath);

    }

    private void addKeyValue(String filePath, String keyValue) throws IOException {
        try (RandomAccessFile storageFile = new RandomAccessFile(filePath, "rw");){
            storageFile.seek(storageFile.length());
            storageFile.write(keyValue.getBytes());
        }
    }

    private void writeLogAppend(KeyValue keyValue, WRITE_ACTION action) {
        writeLogLock.writeLock().lock();
        try {
            String prefix = "";
            switch (action) {
                case PUT:
                    prefix = "P";
                    break;
                case UPDATE:
                    prefix = "U";
                    break;
                case DELETE:
                    prefix = "D";
                    break;
            }
            ;

            String output = prefix + keyValue.getJsonKV();

            try (
                RandomAccessFile writer = new RandomAccessFile(this.writeLogPath, "rw");
            ) {
                writer.setLength(0);
                writer.write(output.getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                logger.error("Could not append to replication write log.");
            }
        } finally {
            writeLogLock.writeLock().unlock();
        }
    }

    private void writeLogAppend(String filePath, WRITE_ACTION action) {
        writeLogLock.writeLock().lock();
        try {
            String prefix = "";
            switch (action) {
                case PUT:
                    prefix = "P";
                    break;
                case UPDATE:
                    prefix = "U";
                    break;
                case DELETE:
                    prefix = "D";
                    break;
            }


            try (
                RandomAccessFile writer = new RandomAccessFile(this.writeLogPath, "rw");
                RandomAccessFile reader = new RandomAccessFile(filePath, "r");
            ) {
                writer.setLength(0);
                String line;
                while ((line = reader.readLine()) != null) {
                    String output = prefix + line + System.lineSeparator();
                    writer.write(output.getBytes(StandardCharsets.UTF_8));
                }
            } catch (IOException e) {
                logger.error("Could not append to replication write log.");
            }
        } finally {
            writeLogLock.writeLock().unlock();
        }
    }

    @Override
    public String get(String key) throws Exception {
        KeyValueLocation keyValueLocation = find(this.filePath, key);

        if (keyValueLocation.isExists()){
            return keyValueLocation.value;
        } else{
            throw new KeyInvalidException(key);
        }
    }

    @Override
    public String get(String key, ServerNode responsibleNode) throws Exception {
        String replicateFilePath = dataDir + File.separatorChar + "repl_" + responsibleNode.getNodeName() + "_" + serverName + ".txt";
        if (replicatedPaths.contains(replicateFilePath)) {
            KeyValueLocation keyValueLocation = find(replicateFilePath, key);
            if (keyValueLocation.isExists()){
                return keyValueLocation.value;
            } else{
                throw new KeyInvalidException(key);
            }
        } else {
            throw new KeyInvalidException(key);
        }
    }

    @Override
    public boolean put(String key, String value) throws Exception {
        return put(this.filePath, key, value, false);
    }

    private boolean put(String filePath, String key, String value, boolean replicate) throws Exception {
        KeyValueLocation keyValueLocation = find(filePath, key);
        KeyValue keyValue = new KeyValue(key, value);

        if (!keyValueLocation.isExists()){
            // Can add to end of file
            addKeyValue(filePath, keyValue.getJsonKV());
            if (!replicate) writeLogAppend(keyValue, WRITE_ACTION.PUT);
        } else{
            // Update existing key-value pair
            updateKeyValue(filePath, keyValueLocation, keyValue.getJsonKV());
            if (!replicate) writeLogAppend(keyValue, WRITE_ACTION.UPDATE);
        }
        return keyValueLocation.isExists();
    }

    @Override
    public boolean exists(String key) throws Exception {
        KeyValueLocation keyValueLocation = find(this.filePath, key);
        return keyValueLocation.isExists();
    }

    @Override
    public void clear() throws IOException {
        try (RandomAccessFile storageFile = new RandomAccessFile(this.filePath, "rw")) {
            storageFile.setLength(0L);
        }
    }

    @Override
    public void delete(String key) throws Exception {
        delete(this.filePath, key, false);
    }

    private void delete(String filePath, String key) throws Exception {
        delete(filePath, key, true);
    }

    private void delete(String filePath, String key, boolean replicator) throws Exception {
        KeyValueLocation keyValueLocation = find(filePath, key);
        KeyValue keyValue = new KeyValue(key);
        if (keyValueLocation.isExists()){
            deleteKeyValue(filePath, keyValueLocation);
            writeLogAppend(keyValue, WRITE_ACTION.DELETE);
        }else{
            if (!replicator) throw new KeyInvalidException(key);
        }
    }

    @Override
    public void mergeData(String newFileName, boolean deleteFile) throws IOException {
        File temp = new File(newFileName);

        try(RandomAccessFile tempRAF = new RandomAccessFile(newFileName, "rw");
            RandomAccessFile storageRAF = new RandomAccessFile(this.filePath, "rw");){
            FileChannel fromChannel = tempRAF.getChannel();
            FileChannel toChannel = storageRAF.getChannel();

            String line;
            while((line = tempRAF.readLine())!= null) {
                logger.info("data received and merging: " + tempRAF.readLine());
            }
            toChannel.position(storageRAF.length());
            fromChannel.position(0L);
            fromChannel.transferTo(0L, fromChannel.size(), toChannel);
        }
        if (deleteFile) temp.delete();
    }

    public void mergeData(String newFileName) throws IOException {
        mergeData(newFileName, true);
    }

    @Override
    public String mergeReplicatedData(ServerNode controller) throws Exception {
        String controlServer = controller.getNodeName();
        String replicateFilePath = dataDir + File.separatorChar + "repl_" + controlServer + "_" + serverName + ".txt";
        mergeData(replicateFilePath, false);
        return replicateFilePath;
    }
    @Override
    public void deleteReplicatedData(ServerNode serverNode) {
        if (serverNode != null) {
            String replicateFilePath = dataDir
                    + File.separatorChar
                    + "repl_"
                    + serverNode.getNodeName()
                    + "_" + serverName + ".txt";

            replicatedPaths.remove(replicateFilePath);
            File replicatedFile = new File(replicateFilePath);
            replicatedFile.delete();
        }

    }

    @Override
    public void replicateData(String tempFilePath, String controlServer) {
        writeLogLock.writeLock().lock();
        try {
            String replicateFilePath = dataDir + File.separatorChar + "repl_" + controlServer + "_" + serverName + ".txt";

            if (replicatedPaths.contains(replicateFilePath)) {
                logger.info("Updating replicated data for " + controlServer);

                updateReplicatedData(tempFilePath, replicateFilePath);
            } else {
                logger.info("Instantiating replicated data for " + controlServer + " at " + replicateFilePath);
                try {
                    // TODO: change this because the first send from server0 to server1 is the write log??
                    // or change the write log sending hmm??
                    if (!Files.exists(Paths.get(replicateFilePath))) Files.createFile(Paths.get(replicateFilePath));
                    updateReplicatedData(tempFilePath, replicateFilePath);
                    logger.info("Instantiated replicated data for " + controlServer);
                    replicatedPaths.add(replicateFilePath);
                } catch (IOException e) {
                    logger.error("Failed to instantiate replicated data for " + controlServer, e);
                }
            }
        } finally {
            writeLogLock.writeLock().unlock();
        }
    }

    private void updateReplicatedData(String tempFilePath, String replicateFilePath){
        //TODO: Check that replication covers the basic cases
        // 1. when bringing on new nodes, data is sent to new server as per M2, but also has to send it to replicators
        // 2. when deleting a node, data is sent to successor ndoe as per M2, but data at replicators of successors is updated too
        // 3. node death?? hasn't been tested at all rn
        try (
            RandomAccessFile reader = new RandomAccessFile(tempFilePath, "r");
        ) {
            Gson gson = new Gson();
            String line;
            while ((line = reader.readLine()) != null) {
                char action =  line.charAt(0);
                String keyValueJson = line.substring(1);

                if (action == 'P') {
                    KeyValue keyValue = gson.fromJson(keyValueJson, KeyValue.class);
                    logger.info("Replicate: put for key: " + keyValue.getKey());
                    addKeyValue(replicateFilePath, keyValueJson + System.lineSeparator());
                } else if (action == 'U') {
                    KeyValue keyValue = gson.fromJson(keyValueJson, KeyValue.class);
                    put(replicateFilePath, keyValue.getKey(), keyValue.getValue(), true);
                    logger.info("Replicate: update for key: " + keyValue.getKey());
                } else if (action == 'D') {
                    KeyValue keyValue = gson.fromJson(keyValueJson, KeyValue.class);
                    logger.info("Replicate: delete for key: " + keyValue.getKey());
                    delete(replicateFilePath, keyValue.getKey());
                } else {
                    KeyValue keyValue = gson.fromJson(line, KeyValue.class);
                    put(replicateFilePath, keyValue.getKey(), keyValue.getValue(), true);
                }
            }
        } catch (Exception e){
            logger.error("Could not update replicated data. ", e);
        }
    }


    public String splitData(String[] keepHashRange) throws IOException {
        File sendFile = new File(this.sendPath);
        boolean ignored = sendFile.createNewFile();
        File keepFile = new File(this.keepPath);
        ignored = keepFile.createNewFile();

        Gson gson = new Gson();

        try(RandomAccessFile storageRAF = new RandomAccessFile(filePath, "r");
            RandomAccessFile sendRAF = new RandomAccessFile(sendPath, "rw");
            RandomAccessFile keepRAF = new RandomAccessFile(keepPath, "rw");){

            String str;

            while ((str = storageRAF.readLine()) != null){
                KeyValue keyValue = gson.fromJson(str, KeyValue.class);
                String keyHash = keyValue.getKeyHash();
                String keyValueJSON = keyValue.getJsonKV();

                if (hashInRange(keyHash, keepHashRange)) {
                    // Key-value is sent
                    sendRAF.seek(sendRAF.length());
                    sendRAF.write(keyValueJSON.getBytes());
                } else {
                    // If key in keyHashRange, key-value is kept
                    keepRAF.seek(keepRAF.length());
                    keepRAF.write(keyValueJSON.getBytes());
                }
            }
        }
        return sendPath;

    }

    public void sendDataCleanup(){
        File storageFile = new File(this.filePath);
        File sendFile = new File(this.sendPath);
        File keepFile = new File(this.keepPath);

        // TODO: don't delete send file, turn it into replication file for the newly created server
        writeLogAppend(this.sendPath, WRITE_ACTION.DELETE);
        sendFile.delete();
        storageFile.delete();
        boolean renamed = keepFile.renameTo(new File(this.filePath));
        if (renamed){
            logger.info("Cleaned up data after sending.");
        } else {
            logger.error("Failed to clean up data after sending, storage deleted.");
        }


    }

    private class KeyValueLocation {
        private String value = null;
        private long startPosition = 0;
        private long endPosition = 0;
        private boolean exists;

        public KeyValueLocation () {
            exists = false;
        }

        public KeyValueLocation (long startPosition, long endPosition, String value) {
            this.startPosition = startPosition;
            this.endPosition = endPosition;
            this.value = value;
            this.exists = true;
        }

        public String getValue() {
            return value;
        }

        public long getStartPosition() {
            return startPosition;
        }

        public long getEndPosition() {
            return endPosition;
        }

        public boolean isExists() {
            return exists;
        }


    }
}

