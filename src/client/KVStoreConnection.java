package client;

import ecs.HashRing;
import ecs.IECSNode;
import shared.Connection;
import shared.messages.DeserializationException;
import shared.messages.JsonKVMessage;
import shared.messages.KVMessage;

import java.net.Socket;

public class KVStoreConnection extends Connection implements KVCommInterface {

	private HashRing hashRing;
	private String currentNodeName;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param hostname the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStoreConnection(String hostname, int port) {
		this.hostname = hostname;
		this.port = port;
	}

	@Override
	public void connect() throws Exception {
		socket = new Socket(hostname, port);
		input = socket.getInputStream();
		output = socket.getOutputStream();
	}

	public void connectToCorrectServer(String key) throws Exception {
		if (hashRing != null) {
			IECSNode responsibleNode = hashRing.getNodeForKey(key);
			if (!responsibleNode.getNodeName().equals(currentNodeName)) {
				this.hostname = responsibleNode.getNodeHost();
				this.port = responsibleNode.getNodePort();
				this.disconnect();
				this.connect();
				this.currentNodeName = responsibleNode.getNodeName();
			}
		}
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		JsonKVMessage req = new JsonKVMessage(KVMessage.StatusType.PUT);
		req.setKey(key);
		req.setValue(value);
		JsonKVMessage res;

		connectToCorrectServer(key);

		try {
			sendMessage(req);
			res = receiveMessage();

			if (res.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
				hashRing = res.getMetadata();
				return put(key, value);
			}
		} catch (DeserializationException e) {
			logger.error(e.getMessage());
			res = new JsonKVMessage(KVMessage.StatusType.PUT_ERROR);
			req.setKey(key);
			req.setValue(value);
			req.setMessage(e.getMessage());
		}

		return res;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		JsonKVMessage req = new JsonKVMessage(KVMessage.StatusType.GET);
		req.setKey(key);
		JsonKVMessage res;

		connectToCorrectServer(key);

		try {
			sendMessage(req);
			res = receiveMessage();

			if (res.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
				hashRing = res.getMetadata();
				return get(key);
			}
		} catch (DeserializationException e) {
			logger.error(e.getMessage());
			res = new JsonKVMessage(KVMessage.StatusType.GET_ERROR);
			res.setKey(key);
			res.setMessage(e.getMessage());
		}

		return res;
	}
}
