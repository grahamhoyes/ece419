package store;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.Set;

import static ecs.ServerNode.hashInRange;

public class KVSimpleStore implements KVStore{
    protected static final Logger logger = Logger.getLogger("KVSimpleStore");
    private static final String dataDir = "store_data";

    private String fileName;
    private String serverName;

    private String filePath;
    private String tempPath;
    private String sendPath;
    private String keepPath;

    private Set<String> replicatedPaths = new HashSet<String>();

    private String value = null;
    private long startPosition = 0;
    private long endPosition = 0;

    public KVSimpleStore(String serverName) throws IOException{
        this.serverName = serverName;
        this.fileName = serverName + "_store.txt";

        this.filePath = dataDir + File.separatorChar + fileName;
        this.tempPath = dataDir + File.separatorChar + "~temp" + this.fileName;
        this.sendPath = dataDir + File.separatorChar + "~send" + this.fileName;
        this.keepPath = dataDir + File.separatorChar + "~keep" + this.fileName;

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

    private boolean find(String key) throws Exception {
        boolean exists = false;
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
                    this.startPosition = prevPointer;
                    this.endPosition = currPointer;
                    this.value = keyValue.getValue();
                    exists = true;
                    break;
                }
                prevPointer = currPointer;
            }
        } catch (JsonSyntaxException e){
            throw new DataFormatException();
        }

        return exists;
    }

    private void deleteKeyValue() throws IOException {
        File temp = new File(tempPath);

        try(RandomAccessFile tempRAFile = new RandomAccessFile(tempPath, "rw");
            RandomAccessFile storageFile = new RandomAccessFile(this.filePath, "rw");){
            FileChannel fromChannel = storageFile.getChannel();
            FileChannel toChannel = tempRAFile.getChannel();

            fromChannel.transferTo(this.endPosition, fromChannel.size(), toChannel);
            storageFile.setLength(startPosition);
            fromChannel.position(startPosition);
            toChannel.transferTo(0, toChannel.size(), fromChannel);

            temp.delete();
        }
    }

    private void updateKeyValue(String keyValue) throws  IOException {
        File temp = new File(tempPath);

        try(RandomAccessFile tempRAFile = new RandomAccessFile(tempPath, "rw");
            RandomAccessFile storageFile = new RandomAccessFile(this.filePath, "rw");){
            FileChannel fromChannel = storageFile.getChannel();
            FileChannel toChannel = tempRAFile.getChannel();

            fromChannel.transferTo(this.endPosition, fromChannel.size(), toChannel);
            storageFile.setLength(startPosition);

            storageFile.seek(startPosition);
            storageFile.write(keyValue.getBytes());
            fromChannel.position(storageFile.getFilePointer());

            toChannel.transferTo(0, toChannel.size(), fromChannel);
        }
        temp.delete();

    }

    private void addKeyValue(String keyValue) throws IOException {
        try (RandomAccessFile storageFile = new RandomAccessFile(filePath, "rw");){
            storageFile.seek(storageFile.length());
            storageFile.write(keyValue.getBytes());
        }
    }

    @Override
    public String get(String key) throws Exception {
        boolean exists = find(key);

        if (exists){
            return this.value;
        } else{
            throw new KeyInvalidException(key);
        }
    }

    @Override
    public boolean put(String key, String value) throws Exception {
        boolean exists = find(key);
        KeyValue keyValue = new KeyValue(key, value);

        if (!exists){
            // Can add to end of file
            addKeyValue(keyValue.getJsonKV());
        } else{
            // Update existing key-value pair
            updateKeyValue(keyValue.getJsonKV());
        }
        return exists;
    }

    @Override
    public boolean exists(String key) throws Exception {
        return find(key);
    }

    @Override
    public void clear() throws IOException {
        try(RandomAccessFile storageFile = new RandomAccessFile(this.filePath, "rw")){
            storageFile.setLength(0L);
        }
    }

    @Override
    public void delete(String key) throws Exception{
        boolean exists = find(key);
        if (exists){
            deleteKeyValue();
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
    public void replicateData(String tempFilePath, String controlServer) {
        File temp = new File(tempFilePath);
        String replicateFilePath = dataDir + File.separatorChar + "repl_" + controlServer + "_" + serverName + ".txt";

        if (replicatedPaths.contains(replicateFilePath)) {
            logger.info("Updating replicated data for " + controlServer);
            updateReplicatedData(tempFilePath, replicateFilePath);
        } else {
            logger.info("Instantiating replicated data for "
                    + controlServer
                    + " at "
                    + replicateFilePath
            );
            temp.renameTo(new File(replicateFilePath));
            replicatedPaths.add(replicateFilePath);
        }
    }

    private void updateReplicatedData(String tempFilePath, String replicateFilePath){
        // TODO: Implement updating replicated data
        // Search through replicated file, copy lines that are not relevant to new file
        // Add updated values to the end of new file
        File temp = new File(tempFilePath);
        temp.renameTo(new File(replicateFilePath));
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

        sendFile.delete();
        storageFile.delete();
        boolean renamed = keepFile.renameTo(new File(this.filePath));
        if (renamed){
            logger.info("Cleaned up data after sending.");
        } else {
            logger.error("Failed to clean up data after sending, storage deleted.");
        }


    }

}

