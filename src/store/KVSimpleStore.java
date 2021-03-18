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
import java.nio.file.StandardCopyOption;
import java.security.Key;
import java.util.HashSet;
import java.util.Set;

import static ecs.ServerNode.hashInRange;

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
        };

        String output = prefix + keyValue.getJsonKV();

        try(
            RandomAccessFile writer = new RandomAccessFile(this.writeLogPath, "rw");
        ) {
            writer.setLength(0);
            writer.write(output.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e){
            logger.error("Could not append to replication write log.");
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
        try(RandomAccessFile storageFile = new RandomAccessFile(this.filePath, "rw")){
            storageFile.setLength(0L);
        }
    }

    @Override
    public void delete(String key) throws Exception {
        delete(this.filePath, key);
    }

    private void delete(String filePath, String key) throws Exception {
        KeyValueLocation keyValueLocation = find(filePath, key);
        KeyValue keyValue = new KeyValue(key);
        if (keyValueLocation.isExists()){
            deleteKeyValue(filePath, keyValueLocation);
            writeLogAppend(keyValue, WRITE_ACTION.DELETE);
        }else{
            throw new KeyInvalidException(key);
        }
    }

    @Override
    public void mergeData(String newFileName) throws IOException {
        File temp = new File(newFileName);

        try(RandomAccessFile tempRAF = new RandomAccessFile(newFileName, "rw");
            RandomAccessFile storageRAF = new RandomAccessFile(this.filePath, "rw");){
            FileChannel fromChannel = tempRAF.getChannel();
            FileChannel toChannel = storageRAF.getChannel();

            toChannel.position(storageRAF.length());
            fromChannel.position(0L);
            fromChannel.transferTo(0L, fromChannel.size(), toChannel);
        }
        temp.delete();
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
        String replicateFilePath = dataDir + File.separatorChar + "repl_" + controlServer + "_" + serverName + ".txt";

        if (replicatedPaths.contains(replicateFilePath)) {
            logger.info("Updating replicated data for " + controlServer);

            updateReplicatedData(tempFilePath, replicateFilePath);
        } else {
            logger.info("Instantiating replicated data for " + controlServer+ " at " + replicateFilePath);
            try {
                Path movedPath = Files.move(
                        Paths.get(tempFilePath),
                        Paths.get(replicateFilePath),
                        StandardCopyOption.REPLACE_EXISTING
                );
                logger.info("Instantiated replicated data for " + controlServer);
                replicatedPaths.add(replicateFilePath);
            } catch (IOException e) {
                logger.error("Failed to instantiate replicated data for " + controlServer, e);
            }
        }
    }

    private void updateReplicatedData(String tempFilePath, String replicateFilePath){
        try (
            RandomAccessFile reader = new RandomAccessFile(tempFilePath, "r");
        ) {
            Gson gson = new Gson();
            String line;
            while ((line = reader.readLine()) != null) {
                char action =  line.charAt(0);
                String keyValueJson = line.substring(1);
                KeyValue keyValue = gson.fromJson(keyValueJson, KeyValue.class);

                if (action == 'P' || action == 'U') {
                    logger.info("Replicate: put/update for key: " + keyValue.getKey());
                    put(replicateFilePath, keyValue.getKey(), keyValue.getValue(), true);
                } else if (action == 'D') {
                    logger.info("Replicate: delete for key: " + keyValue.getKey());
                    delete(replicateFilePath, keyValue.getKey());
                }
            }
        } catch (Exception e){
            logger.error("Could not update replicated data. ", e);
        }
    }


    public String splitData(String[] keyHashRange) throws IOException {
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

                if (hashInRange(keyHash, keyHashRange)) {
                    // If key in keyHashRange, key-value is kept
                    keepRAF.seek(keepRAF.length());
                    keepRAF.write(keyValueJSON.getBytes());
                } else {
                    // Key-value is sent
                    sendRAF.seek(sendRAF.length());
                    sendRAF.write(keyValueJSON.getBytes());
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

