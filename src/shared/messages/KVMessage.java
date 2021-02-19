package shared.messages;

public interface KVMessage {
	
	public enum StatusType {
		GET, 			/* Get - request */
		GET_ERROR, 		/* requested tuple (i.e. value) not found */
		GET_SUCCESS, 	/* requested tuple (i.e. value) found */
		PUT, 			/* Put - request */
		PUT_SUCCESS, 	/* Put - request successful, tuple inserted */
		PUT_UPDATE, 	/* Put - request successful, i.e. value updated */
		PUT_ERROR, 		/* Put - request not successful */
		DELETE_SUCCESS, /* Delete - request successful */
		DELETE_ERROR,	/* Delete - request successful */
		BAD_REQUEST,    /* Any - bad request */
		SERVER_ERROR,   /* Server error closes connection */
		SERVER_WRITE_LOCKED, /* Server locked for write, only get possible */
	}

	public void setStatus(StatusType status);

	public void setKey(String key);

	public void setValue(String value);

	public void setMessage(String message);

	public String getMessage();

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	public String getKey();
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	public String getValue();
	
	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus();

	/**
	* @return a string representing the entire message object
	*/
	public String serialize();

	/**
	 * @param str a string encoding of the message object
	 * @throws DeserializationException when something went wrong with deserialization
	 */
	public void deserialize(String str) throws DeserializationException;
}


