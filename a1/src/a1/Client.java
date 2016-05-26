package a1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.thrift.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

public class Client {
    public static void main(String [] args) {
        try {
            if (args.length != 1) {
                System.err.println("Usage: java a1.Client config_file");
                System.exit(-1);
            }

            BufferedReader br = new BufferedReader(new FileReader(args[0]));
            HashMap<Integer, String> hosts = new HashMap<Integer, String>();
            HashMap<Integer, Integer> ports  = new HashMap<Integer, Integer>();
            String line;
            int i = 0;


            System.out.println("check point 0");


            while ((line = br.readLine()) != null) {
                String[] parts = line.split(" ");
                hosts.put(i, parts[0]);
                ports.put(i, Integer.parseInt(parts[1]));
                i++;
            }

            HashMap<Integer, KeyValueService.Client> client = new HashMap<Integer, KeyValueService.Client>();
            //HashMap<Integer, TTransport> transport = new HashMap<Integer, TTransport>();
            

            System.out.println("check point 1");
            
            for ( int j=0; j<i; j++ ) {
                TSocket sock = new TSocket(hosts.get(j), ports.get(j));
                TTransport transport = new TFramedTransport(sock);
                TProtocol protocol = new TBinaryProtocol(transport);
                KeyValueService.Client c = new KeyValueService.Client(protocol);
                transport.open();
                client.put(j, c);
            }


            System.out.println("check point 2");

            KeyValueService.Client c = client.get(0);
            String[] names = new String[] {"Frank", "Jason", "Eddy"};
            String[] desired = new String[] {"Eddy"};
            String[] errname = new String[] {"Kevin"};
            ArrayList<ByteBuffer> values = new ArrayList<ByteBuffer>();
            byte one[] = {1};
            byte zero[] = {0};
            values.add(ByteBuffer.wrap(one));
            values.add(ByteBuffer.wrap(zero));
            values.add(ByteBuffer.wrap(one));

            c.multiPut(Arrays.asList(names),  values);

            System.out.println("check point 3");

            List<ByteBuffer> ret = c.multiGet(Arrays.asList(desired));
            System.out.println(ret.get(0).equals(ByteBuffer.wrap(one)));

            c.multiGet(Arrays.asList(errname));

        } catch (IllegalArgument ia) {
            System.err.println(ia.message);
        } catch (TException x) {
            x.printStackTrace();
        } catch (IOException io) {

        } finally {
            System.out.println("closing connection");
        } 

    }
}
