package client;

import shared.Connection;
import shared.messages.DeserializationException;
import shared.messages.JsonKVMessage;
import shared.messages.KVMessage;

import java.io.IOException;
import java.net.Socket;

public class KVStoreConnection extends Connection implements KVCommInterface {

	private final String address;
	private final int port;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStoreConnection(String address, int port) {
		this.address = address;
		this.port = port;
	}

	@Override
	public void connect() throws Exception {
		socket = new Socket(address, port);
		input = socket.getInputStream();
		output = socket.getOutputStream();
	}

	@Override
	public void disconnect() {
		try {
			input.close();
			output.close();
			socket.close();
		} catch (IOException e) {
			logger.error("Error! Unable to close connection", e);
		}
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		JsonKVMessage req = new JsonKVMessage(KVMessage.StatusType.PUT, key, value);
		JsonKVMessage res;

		try {
			sendMessage(req);
			res = receiveMessage();
		} catch (RuntimeException e) {
			// RuntimeException is caused by failed message deserialization
			logger.error(e.getMessage());
			res = new JsonKVMessage(KVMessage.StatusType.PUT_ERROR, key, value);
		}

		return res;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		JsonKVMessage req = new JsonKVMessage(KVMessage.StatusType.GET, key);
		JsonKVMessage res;

		try {
			sendMessage(req);
			res = receiveMessage();
		} catch (DeserializationException e) {
			logger.error(e.getMessage());
			res = new JsonKVMessage(KVMessage.StatusType.GET_ERROR, key);
		}

		return res;
	}
}
