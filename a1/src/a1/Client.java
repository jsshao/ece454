package a1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.nio.ByteBuffer;

import org.apache.thrift.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

public class Client {
    public static void main(String [] args) {
        try {
            TSocket sock = new TSocket(args[0], Integer.parseInt(args[1]));
            TTransport transport = new TFramedTransport(sock);
            TProtocol protocol = new TBinaryProtocol(transport);
            KeyValueService.Client client = new KeyValueService.Client(protocol);
            transport.open();

            String[] names = new String[] {"Frank", "Jason", "Eddy"};
            String[] desired = new String[] {"Eddy"};
            String[] errname = new String[] {"Kevin"};
            ArrayList<ByteBuffer> values = new ArrayList<ByteBuffer>();
            byte one[] = {1};
            byte zero[] = {0};
            values.add(ByteBuffer.wrap(one));
            values.add(ByteBuffer.wrap(zero));
            values.add(ByteBuffer.wrap(one));

            client.multiPut(Arrays.asList(names),  values);

            List<ByteBuffer> ret = client.multiGet(Arrays.asList(desired));
            System.out.println(ret.get(0).equals(ByteBuffer.wrap(one)));

            client.multiGet(Arrays.asList(errname));

            transport.close();
        } catch (IllegalArgument ia) {
            System.out.println(ia.message);
        } catch (TException x) {
            x.printStackTrace();
        } 
    }
}
