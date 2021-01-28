package store;

import com.google.gson.Gson;
import org.apache.log4j.Logger;
import store.KeyInvalidException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public class KVSimpleStore implements KVStore{
    protected static final Logger logger = Logger.getRootLogger();

    private String fileName;
    private String tempPath;

    private String value = null;
    private long startPosition = 0;
    private long endPosition = 0;

    public KVSimpleStore(String fileName) throws IOException{
        this.fileName = fileName;
        this.tempPath = "~temp" + this.fileName;

        prepareFile();
    }

    private void prepareFile() throws IOException{
        File storageFile = new File(this.fileName);
        boolean fileCreated = storageFile.createNewFile();
        if (fileCreated){
            logger.info("KVSimpleStore: Storage file created.");
        } else {
            logger.info("KVSimpleStore: Storage file found.");
        }
    }

    private boolean find(String key) throws IOException{
        boolean exists = false;
        Gson gson = new Gson();

        try(RandomAccessFile storageFile = new RandomAccessFile(fileName, "r");) {
            String str = null;

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
        }

        return exists;
    }

    private void deleteKeyValue() throws IOException{
        File temp = new File(tempPath);

        try(RandomAccessFile tempRAFile = new RandomAccessFile(tempPath, "rw");
            RandomAccessFile storageFile = new RandomAccessFile(this.fileName, "rw");){
            FileChannel fromChannel = storageFile.getChannel();
            FileChannel toChannel = tempRAFile.getChannel();

            fromChannel.transferTo(this.endPosition, fromChannel.size(), toChannel);
            storageFile.setLength(startPosition);
            fromChannel.position(startPosition);
            toChannel.transferTo(0, toChannel.size(), fromChannel);

            temp.delete();
        }
    }

    private void updateKeyValue(String keyValue) throws  IOException{
        File temp = new File(tempPath);

        try(RandomAccessFile tempRAFile = new RandomAccessFile(tempPath, "rw");
            RandomAccessFile storageFile = new RandomAccessFile(this.fileName, "rw");){
            FileChannel fromChannel = storageFile.getChannel();
            FileChannel toChannel = tempRAFile.getChannel();

            fromChannel.transferTo(this.endPosition, fromChannel.size(), toChannel);
            storageFile.setLength(startPosition);

            storageFile.seek(startPosition);
            storageFile.write(keyValue.getBytes());
            fromChannel.position(storageFile.getFilePointer());

            toChannel.transferTo(0, toChannel.size(), fromChannel);

            temp.delete();
        }

    }

    public String get(String key) throws KeyInvalidException, IOException {
        boolean exists = find(key);

        if (exists){
            return this.value;
        } else{
            throw new KeyInvalidException(key);
        }
    }

    public boolean put(String key, String value) throws KeyInvalidException, IOException{
        boolean exists = find(key);
        KeyValue keyValue = new KeyValue(key, value);

        if (!exists){
            if (value.equals("null")){
                throw new KeyInvalidException(key);
            }
            // Can add to end of file
            try (RandomAccessFile storageFile = new RandomAccessFile(fileName, "rw");){
                Gson gson = new Gson();
                storageFile.seek(storageFile.length());
                storageFile.write(keyValue.getJsonKV().getBytes());
            }
        } else{
            if (value.equals("null")){
                deleteKeyValue();
            } else{
                Gson gson = new Gson();
                updateKeyValue(keyValue.getJsonKV());
            }

        }
        return exists;
    }

    public boolean exists(String key) throws Exception{
        return find(key);
    }

    public void clear() throws IOException{
        try(RandomAccessFile storageFile = new RandomAccessFile(this.fileName, "rw")){
            storageFile.setLength(0L);
        }
    }

}

