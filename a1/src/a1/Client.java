package a1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.HashMap;
import java.util.Random;
import java.util.UUID;
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

            while ((line = br.readLine()) != null) {
                String[] parts = line.split(" ");
                hosts.put(i, parts[0]);
                ports.put(i, Integer.parseInt(parts[1]));
                i++;
            }

            HashMap<Integer, KeyValueService.Client> client = new HashMap<Integer, KeyValueService.Client>();
            
            for ( int j=0; j<i; j++ ) {
                TSocket sock = new TSocket(hosts.get(j), ports.get(j));
                TTransport transport = new TFramedTransport(sock);
                TProtocol protocol = new TBinaryProtocol(transport);
                KeyValueService.Client c = new KeyValueService.Client(protocol);
                transport.open();
                client.put(j, c);
            }

            Random r = new Random();

            // Correctness test for each client 100 times
            long correctness_duration = 0;
            for (int j = 0; j < i; j++) {
                KeyValueService.Client c = client.get(j);

                // Repeat request 100 times
                for (int request = 0; request < 100; request++) {
                    List<String> keys = new ArrayList<String>();
                    List<ByteBuffer> values = new ArrayList<ByteBuffer>();

                    // Create random key-value pairs
                    for (int k = 0; k < 1000; k++) {
                        String key = UUID.randomUUID().toString();
                        keys.add(key);
                        byte value[] = new byte[100];
                        new Random().nextBytes(value);
                        values.add(ByteBuffer.wrap(value));
                    }
                    List<ByteBuffer> ret;
                    long startTime = System.currentTimeMillis();
                    ret = c.multiPut(keys, values);
                    correctness_duration += System.currentTimeMillis() - startTime;
                    for (ByteBuffer byteBuf: ret) {
                        byte[] arr = new byte[byteBuf.remaining()];
                        byteBuf.get(arr);
                        if (arr.length != 0)
                            System.out.println("ERROR: Expected length 0 but got " + arr.length);
                    }
                    //List<ByteBuffer> ret = c.multiGet(keys);
                }
            }

            System.out.println("Correctness Test Finished");
            System.out.println("Average Latency: " + correctness_duration / 100.0 / i);

            // Stress each client with with 100 requests of maximum size
            long stress_duration = 0;
            for (int j = 0; j < i; j++) {
                KeyValueService.Client c = client.get(j);

                // Repeat request 100 times
                for (int request = 0; request < 100; request++) {
                    List<String> keys = new ArrayList<String>();
                    List<ByteBuffer> values = new ArrayList<ByteBuffer>();

                    // Create random key-value pairs
                    for (int k = 0; k < 1000; k++) {
                        String key = new String(new char[999]).replace("\0", "a") + alphabet.charAt(r.nextInt(N));
                        keys.add(key);
                        byte value[] = new byte[100];
                        new Random().nextBytes(value);
                        values.add(ByteBuffer.wrap(value));
                    }
                    long startTime = System.currentTimeMillis();
                    c.multiPut(keys, values);
                    List<ByteBuffer> ret = c.multiGet(keys);
                    stress_duration += System.currentTimeMillis() - startTime;
                }
            }
            System.out.println("Stress Test Finished");
            System.out.println("Average Latency: " + stress_duration / 100.0 / i / 2);
        } catch (IllegalArgument ia) {
            System.err.println(ia.message);
        } catch (TException x) {
            x.printStackTrace();
        } catch (IOException io) {

        } finally { System.out.println("closing connection");
        } 
    }
}
