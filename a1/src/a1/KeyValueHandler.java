package a1;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;

public class KeyValueHandler implements KeyValueService.Iface {
    private ConcurrentHashMap<String, ByteBuffer> map = new ConcurrentHashMap<String, ByteBuffer>();

    public List<String> getGroupMembers()
    {
        List<String> ret = new ArrayList<String>();
        ret.add("jsshao");
        ret.add("mhlu");
        return ret;
    }

    public List<ByteBuffer> multiGet(List<String> keys) {
        List<ByteBuffer> values = new ArrayList<ByteBuffer>(keys.size()); 
        for (String key : keys) {
            values.add(map.containsKey(key) ? map.get(key) : ByteBuffer.allocate(0));
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

            oldValues.add(map.containsKey(key) ? map.get(key) : ByteBuffer.allocate(0));
            map.put(key, value);
        }

        return oldValues;
    }
}
