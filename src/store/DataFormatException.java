package store;

public class DataFormatException extends Exception{
    public DataFormatException () {
        super("Incorrect or corrupted data format in storage file.");
    }
}

