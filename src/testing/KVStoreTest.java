package testing;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import store.DataFormatException;
import store.KVSimpleStore;
import store.KVStore;
import store.KeyInvalidException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class KVStoreTest extends Assert {
    private static KVStore kvStore;
    private static final String fileName = "testing.txt";

    @BeforeClass
    public static void init() throws IOException {
        kvStore = new KVSimpleStore(fileName);
    }

    @Before
    public void clear() throws Exception{
        kvStore.clear();
    }

    @Test(expected = KeyInvalidException.class)
    public void testInvalidGetKey() throws Exception{
        String key = "foo";
        kvStore.get(key);
    }

    @Test(expected = KeyInvalidException.class)
    public void testInvalidDeleteKey() throws Exception{
        String key = "foo";
        kvStore.delete(key);
    }

    @Test
    public void testPutGet() throws Exception{
        String key = "foo";
        String value = "bar";
        kvStore.put(key, value);
        String getValue = kvStore.get(key);

        assertEquals(value, getValue);
    }

    @Test
    public void testPutUpdate() throws Exception{
        String key = "foo";
        String value = "bar";
        String updateValue = "fizz";

        kvStore.put(key, value);
        String oldValue = kvStore.get(key);
        kvStore.put(key, updateValue);
        String newValue = kvStore.get(key);

        assertEquals(updateValue, newValue);
        assertFalse(updateValue.equals(oldValue));

    }

    @Test
    public void testExists() throws Exception{
        String key = "foo";
        String value = "bar";

        kvStore.put(key, value);
        boolean exists = kvStore.exists(key);

        assertTrue(exists);
    }

    @Test
    public void testDelete() throws Exception{
        String key = "foo";
        String value = "bar";

        kvStore.put(key, value);
        kvStore.delete(key);
        boolean exists = kvStore.exists(key);

        assertFalse(exists);
    }

    @Test (expected = FileNotFoundException.class)
    public void testFileDeletion() throws Exception{
        File file = new File(kvStore.getDataDir() + File.separatorChar + fileName);
        file.delete();
        try{
            String key = "foo";
            String value = "bar";
            kvStore.put(key, value);
        }finally {
            file.createNewFile();
        }
    }

    @Test (expected = DataFormatException.class)
    public void testInvalidDataFormat() throws Exception{
        String garbage = "garbage";
        Files.writeString(
                Paths.get(kvStore.getDataDir() + File.separatorChar + fileName),
                garbage,
                StandardCharsets.ISO_8859_1,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);

        String key = "foo";
        kvStore.exists(key);
    }



}
