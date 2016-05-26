package a1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessorFactory;

public class StorageNode {
    public static void main(String [] args) throws Exception {
        org.apache.log4j.BasicConfigurator.configure();
        if (args.length != 2) {
            System.err.println("Usage: java a1.StorageNode config_file node_num");
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
        int myNum = Integer.parseInt(args[1]);
        System.out.println("My host and port: " + hosts.get(myNum) + ":" + ports.get(myNum));

        // Launch a Thrift server here
/*        try {
            KeyValueService.Processor processor = 
                new KeyValueService.Processor(new KeyValueHandler(hosts, ports, myNum));
            TServerSocket socket = new TServerSocket(ports.get(myNum));
            TSimpleServer.Args sargs = new TSimpleServer.Args(socket);
            sargs.protocolFactory(new TBinaryProtocol.Factory());
            sargs.transportFactory(new TFramedTransport.Factory());
            sargs.processorFactory(new TProcessorFactory(processor));
            TServer server = new TSimpleServer(sargs);
            server.serve();
*/
        try {
            KeyValueService.Processor processor = 
                new KeyValueService.Processor(new KeyValueHandler(hosts, ports, myNum));
            TServerSocket socket = new TServerSocket(ports.get(myNum));
            TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
            sargs.protocolFactory(new TBinaryProtocol.Factory());
            sargs.transportFactory(new TFramedTransport.Factory());
            sargs.processorFactory(new TProcessorFactory(processor));
            TThreadPoolServer server = new TThreadPoolServer(sargs);
            server.serve();
        } catch (TException x) {
            // How to propagate?
            x.printStackTrace();
            throw x;
        }
    }
}
