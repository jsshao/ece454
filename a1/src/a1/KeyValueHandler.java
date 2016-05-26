package a1;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import org.apache.thrift.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

public class KeyValueHandler implements KeyValueService.Iface {
    private ConcurrentHashMap<String, ByteBuffer> map = new ConcurrentHashMap<String, ByteBuffer>();
    private HashMap<Integer, String> mHosts;
    private HashMap<Integer, Integer> mPorts;
    private ConnectionPool mPool;
    private int mServerId;
    private int mNumOfServers;

    public KeyValueHandler(HashMap<Integer, String> hosts, HashMap<Integer, Integer> ports, int serverId) {
        mHosts = hosts;
        mPorts = ports; 
        mPool = new ConnectionPool(hosts, ports);
        mNumOfServers = hosts.size();
        mServerId = serverId;
    }

    public List<String> getGroupMembers() {
        List<String> ret = new ArrayList<String>();
        ret.add("jsshao");
        ret.add("mhlu");
        return ret;
    }

    public List<ByteBuffer> multiGet(List<String> keys) {
        List<ByteBuffer> values = new ArrayList<ByteBuffer>(keys.size()); 
        HashMap<Integer, ArrayList<String>> batches = new HashMap<Integer, ArrayList<String>>();
        HashMap<Integer, ArrayList<Integer>> batchStringIds = new HashMap<Integer, ArrayList<Integer>>();
        HashMap<Integer, ByteBuffer> resMap = new HashMap<Integer, ByteBuffer>();
        for (int i=0; i<keys.size(); i++) {
            String key = keys.get(i);
            int expectedServer = key.hashCode() % mNumOfServers;

            if ( expectedServer == mServerId ) {
                resMap.put(i, map.containsKey(key) ? map.get(key) : ByteBuffer.allocate(0));
                continue;
            }

            if ( !batches.containsKey(expectedServer) ) {
                batches.put(expectedServer, new ArrayList<String>());
                batchStringIds.put(expectedServer, new ArrayList<Integer>());
            }
            List<String> batchesKeys = batches.get(expectedServer);
            List<Integer> ids = batchStringIds.get(expectedServer);
            batchesKeys.add(key);
            ids.add(i);
        }

        for (Map.Entry<Integer, ArrayList<String>> entry : batches.entrySet()) {
            Integer serverId = entry.getKey();
            List<String> batchKeys = entry.getValue();
            List<Integer> ids = batchStringIds.get(serverId);
            List<ByteBuffer> remoteRes = getRemote(batchKeys, serverId);
            for (int i=0; i<remoteRes.size(); i++) {
                resMap.put(ids.get(i), remoteRes.get(i));
            }
        }

        for (int i=0; i<keys.size(); i++) {
            values.add(resMap.get(i));
        }

        return values;
    }

    public List<ByteBuffer> multiPut(List<String> keys, List<ByteBuffer> values) throws IllegalArgument {
        if (keys.size() != values.size()) {
            throw new IllegalArgument("List of keys does not have same length as list of values");
        }

        List<ByteBuffer> oldValues = new ArrayList<ByteBuffer>(keys.size());

        for (int i = 0; i < keys.size(); i++) {
            String key = keys.get(i);
            ByteBuffer value = values.get(i);
            int expectedServer = key.hashCode() % mNumOfServers;

            if (expectedServer == mServerId) {
                oldValues.add(map.containsKey(key) ? map.get(key) : ByteBuffer.allocate(0));
                map.put(key, value); 
            } else {
                oldValues.add(putRemote(key, value));
            }
        }
        return oldValues;
    }

    private List<ByteBuffer> getRemote(List<String> keys, Integer server) {
        System.out.println("Making remote get call to " + server);
        try {
            KeyValueService.Client client = mPool.getConnection(server);
            List<ByteBuffer> ret = client.multiGet(keys);
            mPool.releaseConnection(server, client);
            return ret;
        } catch (TException x) {
            x.printStackTrace();
        }
    }

    private ByteBuffer putRemote(String key, ByteBuffer value) {
        System.out.println("Making remote put call to " + key);
        try {
            int expectedServer = key.hashCode() % mNumOfServers;
            KeyValueService.Client client = mPool.getConnection(expectedServer);

            List<String> keyList = new ArrayList<String>();
            keyList.add(key);
            List<ByteBuffer> valueList = new ArrayList<ByteBuffer>();
            valueList.add(value);
            System.out.println(client);
            List<ByteBuffer> ret = client.multiPut(keyList, valueList);

            mPool.releaseConnection(expectedServer, client);
            return ret.get(0);
        } catch (TException x) {
            x.printStackTrace();
        }
        return ByteBuffer.allocate(0);
    }
}
