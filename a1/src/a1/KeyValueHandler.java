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
    private int mServerId;
    private int mNumOfServers;

    public KeyValueHandler(HashMap<Integer, String> hosts, HashMap<Integer, Integer> ports, int serverId) {
        mHosts = hosts;
        mPorts = ports; 
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
        for (String key : keys) {
            int expectedServer = key.hashCode() % mNumOfServers;
            if (expectedServer == mServerId) {
                values.add(map.containsKey(key) ? map.get(key) : ByteBuffer.allocate(0));
            } else {
                values.add(getRemote(key));
            }
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
        try {
            int expectedServer = key.hashCode() % mNumOfServers;
            TSocket sock = new TSocket(mHosts.get(expectedServer), mPorts.get(expectedServer));
            TTransport transport = new TFramedTransport(sock);
            TProtocol protocol = new TBinaryProtocol(transport);
            KeyValueService.Client client = new KeyValueService.Client(protocol);
            transport.open();

            List<String> keyList = new ArrayList<String>();
            keyList.add(key);
            List<ByteBuffer> ret = client.multiGet(keyList);
            transport.close();
            return ret.get(0);
        } catch (TException x) {
            x.printStackTrace();
        }
        return ByteBuffer.allocate(0);
    }

    private ByteBuffer putRemote(String key, ByteBuffer value) {
        try {
            int expectedServer = key.hashCode() % mNumOfServers;
            TSocket sock = new TSocket(mHosts.get(expectedServer), mPorts.get(expectedServer));
            TTransport transport = new TFramedTransport(sock);
            TProtocol protocol = new TBinaryProtocol(transport);
            KeyValueService.Client client = new KeyValueService.Client(protocol);
            transport.open();

            List<String> keyList = new ArrayList<String>();
            keyList.add(key);
            List<ByteBuffer> valueList = new ArrayList<ByteBuffer>();
            valueList.add(value);
            List<ByteBuffer> ret = client.multiPut(keyList, valueList);
            transport.close();
            return ret.get(0);
        } catch (TException x) {
            x.printStackTrace();
        }
        return ByteBuffer.allocate(0);
    }
}
