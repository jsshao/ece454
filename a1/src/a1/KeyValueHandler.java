package a1;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
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
        HashMap<Integer, ArrayList<String>> batch = new HashMap<Integer, ArrayList<String>>();
        HashMap<Integer, ArrayList<Integer>> stringId = new HashMap<Integer, ArrayList<Integer>>();
        for (int i=0; i<keys.size(); i++) {
            String key = keys.get(i);
            int expectedServer = key.hashCode() % mNumOfServers;

            if ( !batch.containsKey(expectedServer) ) {
                batch.put(expectedServer, new ArrayList<String>);
                stringId.put(expectedServer, new ArrayList<Integer>);
            }
            ArrayList<String> batchKeys = batch.get(expectedServer);
            ArrayList<Integer> id = stringId.get(expectedServer);
            batchKeys.add(key);
            id.add(i);
        }

        HashMap<Integer, String> resMap = new HashMap<Integer, String>();
        for (Map.Entry<String, Object> entry : batch.entrySet()) {
            String serverId = entry.getKey();
            ArrayList<String> batchKeys = entry.getValue();
            ArrayList<Integer> id = stringId.get(serverId);
            List<ByteBuffer> remoteRes = getRremote(batchKeys);
            for (int i=0; i<remoteRes.size(); i++) {
                resMap.put(id.get(i), remoteRes.get(i));
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

    private ByteBuffer getRemote(String key) {
        System.out.println("Making remote get call to " + key);
        try {
            int expectedServer = key.hashCode() % mNumOfServers;
            KeyValueService.Client client = mPool.getConnection(expectedServer);

            List<String> keyList = new ArrayList<String>();
            keyList.add(key);
            List<ByteBuffer> ret = client.multiGet(keyList);

            mPool.releaseConnection(expectedServer, client);
            return ret.get(0);
        } catch (TException x) {
            x.printStackTrace();
        }
        return ByteBuffer.allocate(0);
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
