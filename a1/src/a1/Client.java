package a1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.HashMap;
import java.util.Random;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.thrift.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

public class Client {
    public static void main(String [] args) {
        final String alphabet = "0123456789ABCDE";
        final int N = alphabet.length();

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
            Random r = new Random();

            for (int j = 0; j < i; j++) {
                KeyValueService.Client c = client.get(j);
                List<String> keys = new ArrayList<String>();
                List<ByteBuffer> values = new ArrayList<ByteBuffer>();
                for (int k = 0; k < 1000; k++) {
                    String key = new String(new char[999]).replace("\0", "a") + alphabet.charAt(r.nextInt(N));
                    keys.add(key);
                    byte value[] = new byte[10000];
                    new Random().nextBytes(value);
                    values.add(ByteBuffer.wrap(value));
                }

                c.multiPut(keys, values);
                List<ByteBuffer> ret = c.multiGet(keys);

                // Check last element which is guaranteed to be same
                System.out.println(ret.get(ret.size() - 1).equals(values.get(values.size() - 1)));
            }
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
