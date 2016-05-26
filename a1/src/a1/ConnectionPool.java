package a1;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.HashMap;

import org.apache.thrift.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

public class ConnectionPool {
    private static int CONNECTION_LIMIT = 2;
    private static int POLL_FREQUENCY_MS = 100;
    private HashMap<Integer, String> mHosts;
    private HashMap<Integer, Integer> mPorts;
    private HashMap<Integer, ConcurrentLinkedQueue<KeyValueService.Client>> mPool;
    private ConcurrentHashMap<Integer, Integer> mNumExistingConnections;

    public ConnectionPool(HashMap<Integer, String> hosts, HashMap<Integer, Integer> ports) {
        mHosts = hosts;
        mPorts = ports;
        mPool = new HashMap<Integer, ConcurrentLinkedQueue<KeyValueService.Client>>();
        mNumExistingConnections = new ConcurrentHashMap<Integer, Integer>();
        for (Integer serverId : hosts.keySet()) {
            mNumExistingConnections.put(serverId, 0);
            mPool.put(serverId, new ConcurrentLinkedQueue<KeyValueService.Client>());
        }
    }

    public KeyValueService.Client getConnection(int serverId) {
        ConcurrentLinkedQueue<KeyValueService.Client> queue = mPool.get(serverId);
        synchronized(queue) {
            if (queue.isEmpty() && mNumExistingConnections.get(serverId) < CONNECTION_LIMIT) {
                System.out.println("Creating new connection");
                mNumExistingConnections.put(serverId, mNumExistingConnections.get(serverId) + 1);
                return createNewClient(serverId);
            } else {
                System.out.println("Waiting for connection to be free");
                while (queue.isEmpty()) {
                    try {
                        Thread.sleep(POLL_FREQUENCY_MS);
                    } catch (InterruptedException x) {
                    }
                }
                KeyValueService.Client client = queue.poll();
                return client;
            }
        }
    }

    public void releaseConnection(int serverId, KeyValueService.Client client) {
        ConcurrentLinkedQueue<KeyValueService.Client> queue = mPool.get(serverId);
        queue.offer(client);
        System.out.println("Putting client back into queue");
    }

    private KeyValueService.Client createNewClient(int serverId) {
        System.out.println("Creating new client with server " + serverId);
        try {
            TSocket sock = new TSocket(mHosts.get(serverId), mPorts.get(serverId));
            TTransport transport = new TFramedTransport(sock);
            TProtocol protocol = new TBinaryProtocol(transport);
            KeyValueService.Client client = new KeyValueService.Client(protocol);
            transport.open();
            return client;
        } catch (TException x) {
            x.printStackTrace();
            return null;
        }
    }
}
