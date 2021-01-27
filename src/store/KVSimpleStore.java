package store;

import com.google.gson.Gson;
import store.exceptions.GetKeyInvalidException;
import store.exceptions.PutKeyInvalidException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public class KVSimpleStore implements KVStore{
    private String fileName;
    private String tempPath;
    private String value = null;
    private long startPosition = 0;
    private long endPosition = 0;

    public KVSimpleStore(String fileName){
        this.fileName = fileName;
        this.tempPath = "~temp" + this.fileName;
    }

    public boolean find(String key) throws IOException{
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

    public void deleteKeyValue() throws IOException{
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

    public void updateKeyValue(String keyValue) throws  IOException{
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

    public String get(String key) throws GetKeyInvalidException, IOException {
        boolean exists = find(key);

        if (exists){
            return this.value;
        }
        else{
            throw new GetKeyInvalidException(key);
        }
    }

    public void put(String key, String value) throws PutKeyInvalidException, IOException{
        boolean exists = find(key);
        KeyValue keyValue = new KeyValue(key, value);

        if (!exists){
            if (value.equals("null")){
                throw new PutKeyInvalidException(key);
            }
            // Can add to end of file
            try (RandomAccessFile storageFile = new RandomAccessFile(fileName, "rw");){
                Gson gson = new Gson();
                storageFile.seek(storageFile.length());
                storageFile.write(keyValue.getJsonKV().getBytes());
            }
        }
        else{
            if (value.equals("null")){
                deleteKeyValue();
            }
            else{
                Gson gson = new Gson();
                updateKeyValue(keyValue.getJsonKV());
            }

        }
    }

    public boolean exists(String key) throws IOException{
        return find(key);
    }

    public void clear() throws IOException{
        try(RandomAccessFile storageFile = new RandomAccessFile(this.fileName, "rw")){
            storageFile.setLength(0L);
        }
    }

}

