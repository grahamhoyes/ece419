package testing;

import org.junit.Test;
import org.junit.Assert;
import shared.messages.DeserializationException;
import shared.messages.JsonKVMessage;
import shared.messages.KVMessage.StatusType;

/**
 * Test for the JsonKVMessage Class
 *
 * Written by Graham Hoyes
 */
public class JsonMessageTest extends Assert {

    @Test
    public void testInitializationFromStatusType() {
        JsonKVMessage msg = new JsonKVMessage(StatusType.GET_SUCCESS);

        assertEquals(msg.getStatus(), StatusType.GET_SUCCESS);
        assertNull(msg.getKey());
        assertNull(msg.getValue());
        assertNull(msg.getMessage());
    }

    @Test
    public void testDeserialization() throws DeserializationException {
        JsonKVMessage msg = new JsonKVMessage();
        String json = "{\"status\": \"PUT_ERROR\", \"key\": \"foo\", \"value\": \"bar\", \"message\": \"Something went wrong\"}";
        msg.deserialize(json);

        assertEquals(msg.getStatus(), StatusType.PUT_ERROR);
        assertEquals(msg.getKey(), "foo");
        assertEquals(msg.getValue(), "bar");
        assertEquals(msg.getMessage(), "Something went wrong");
    }

    @Test(expected = DeserializationException.class)
    public void testDeserializationFails() throws DeserializationException {
        String json = "{I am not valid json";
        JsonKVMessage msg = new JsonKVMessage();
        msg.deserialize(json);
    }

    @Test
    public void testInitializationFromJsonString() throws DeserializationException {
        String json = "{\"status\": \"PUT_ERROR\", \"key\": \"foo\", \"value\": \"bar\", \"message\": \"Something went wrong\"}";
        JsonKVMessage msg = new JsonKVMessage(json);

        assertEquals(msg.getStatus(), StatusType.PUT_ERROR);
        assertEquals(msg.getKey(), "foo");
        assertEquals(msg.getValue(), "bar");
        assertEquals(msg.getMessage(), "Something went wrong");
    }

    @Test
    public void testDeserializationHandlesMissingFields() throws DeserializationException {
        String json = "{\"status\": \"PUT_SUCCESS\", \"key\": \"foo\"}";
        JsonKVMessage msg = new JsonKVMessage(json);

        assertEquals(msg.getStatus(), StatusType.PUT_SUCCESS);
        assertEquals(msg.getKey(), "foo");
        assertNull(msg.getValue());
        assertNull(msg.getMessage());
    }

    @Test(expected = DeserializationException.class)
    public void testDeserializationRequiresStatusType() throws DeserializationException {
        String json = "{\"key\": \"foo\", \"value\": \"bar\", \"message\": \"Something went wrong\"}";
        new JsonKVMessage(json);
    }

}
