package client;

import ecs.ServerNode;
import ecs.HashRing;
import org.apache.log4j.Logger;
import shared.Connection;
import shared.messages.DeserializationException;
import shared.messages.JsonKVMessage;
import shared.messages.KVMessage;

import java.io.IOException;
import java.net.Socket;

public class KVStoreConnection extends Connection implements KVCommInterface {
	public static Logger logger = Logger.getLogger("KVStoreConnection");
	private HashRing hashRing;
	private ServerNode currentNode;
	private boolean retry = false;
	private int retryAttempts = 0;

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

	private void switchServers(ServerNode node) throws Exception {
		this.hostname = node.getNodeHost();
		this.port = node.getNodePort();
		this.disconnect();
		this.connect();
	}

	private void switchToSuccessor() throws Exception {
		if (hashRing == null) return;

		ServerNode successor = currentNode.getSuccessor();
		this.currentNode = successor;
		switchServers(successor);
	}

	@Override
	public KVMessage putTTL(String key, String value, Long ttl) throws Exception {
		JsonKVMessage req = new JsonKVMessage(KVMessage.StatusType.PUT);
		req.setKey(key);
		req.setValue(value);
		req.setTTL(ttl);
		JsonKVMessage res;

		if (!retry) {
			if (hashRing != null) {

				ServerNode responsibleNode = hashRing.getNodeForKey(key);
				if (currentNode == null || !currentNode.isNodeResponsible(key)) {
					switchServers(responsibleNode);
					this.currentNode = responsibleNode;
				}
			}

			retryAttempts = 0;
		} else {
			switchToSuccessor();
			retryAttempts++;
		}

		try {
			sendMessage(req);
			res = receiveMessage();
			hashRing = res.getMetadata();
			if (hashRing != null && currentNode != null) {
				currentNode = hashRing.getNode(currentNode.getNodeName());
			}

			retry = false;
			if (res.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
				// Metadata updated above, retry with new metadata
				return putTTL(key, value, ttl);
			}
		} catch (DeserializationException e) {
			logger.error(e.getMessage());
			res = new JsonKVMessage(KVMessage.StatusType.PUT_ERROR);
			req.setKey(key);
			req.setValue(value);
			req.setTTL(ttl);
			req.setMessage(e.getMessage());

		} catch (IOException e) {
			if (!e.getMessage().equals("Connection terminated"))
				throw e;

			// Server has terminated the connection
			if (hashRing == null) throw e;

			if (retryAttempts == hashRing.getNodes().size()) {
				retry = false;
				throw e;
			} else {
				retry = true;
				return putTTL(key, value, ttl);
			}
		}

		return res;
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		return this.putTTL(key, value, null);
	}

	@Override
	public KVMessage get(String key) throws Exception {
		JsonKVMessage req = new JsonKVMessage(KVMessage.StatusType.GET);
		req.setKey(key);
		JsonKVMessage res;

		if (!retry) {
			if (hashRing != null) {

				ServerNode responsibleNode = hashRing.getNodeForKey(key);
				if (currentNode == null || (!currentNode.doesNodeReplicateKey(key) && !currentNode.isNodeResponsible(key))) {
					switchServers(responsibleNode);
					this.currentNode = responsibleNode;
				}
			}

			retryAttempts = 0;
		} else {
			switchToSuccessor();
			retryAttempts++;
		}

		try {
			sendMessage(req);
			res = receiveMessage();
			// TODO: Have a way to request metadata from the server after retries,
			//  rather than always sending it to save network traffic
			hashRing = res.getMetadata();
			if (hashRing != null && currentNode != null) {
				currentNode = hashRing.getNode(currentNode.getNodeName());
			}

			retry = false;
			if (res.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE) {
				// Metadata updated above, retry with new metadata
				return get(key);
			}
		} catch (DeserializationException e) {
			logger.error(e.getMessage());
			res = new JsonKVMessage(KVMessage.StatusType.GET_ERROR);
			res.setKey(key);
			res.setMessage(e.getMessage());
		} catch (IOException e) {
			if (!e.getMessage().equals("Connection terminated"))
				throw e;

			// Server has terminated the connection
			if (hashRing == null) throw e;

			if (retryAttempts == hashRing.getNodes().size()) {
				retry = false;
				throw e;
			} else {
				retry = true;
				return get(key);
			}
		}

		return res;
	}
}
