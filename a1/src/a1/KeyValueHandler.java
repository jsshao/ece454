package a1;

import java.util.ArrayList;
import java.util.List;

public class KeyValueHandler implements KeyValueService.Iface {
    public List<String> getGroupMembers()
    {
	List<String> ret = new ArrayList<String>();
	ret.add("nexusid1");
	ret.add("nexusid2");
	return ret;
    }
}
