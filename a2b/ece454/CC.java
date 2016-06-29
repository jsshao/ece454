package ece454;

import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import java.nio.*;
import java.nio.channels.*;

public class CC {
    public static int closestIndex(byte [] buffer, int offset) {
        for (int i = offset; i >= 0; i--) {
            if (buffer[i] == '\n')
                return i;
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int numThreads = Integer.parseInt(args[0]);

        // read graph from input file
        FileInputStream fis = new FileInputStream(args[1]);
        FileChannel fc = fis.getChannel();
        MappedByteBuffer mmb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
        byte[] buffer = new byte[(int)fc.size()];
        mmb.get(buffer);
        fis.close();

        // Don't bother multi-threading for small graphs
        if (buffer.length < 1000) {
            numThreads = 1;
        }

        UnionFind<Integer> u = new UnionFind<Integer>();

        // start worker threads
        int thd_buf_length = buffer.length / numThreads;
        int prev = 0;
        int next;
        Thread[] threads = new Thread[numThreads];
        Object lock = new Object();
        for (int t = 0; t < numThreads; t++) {
            if (t == numThreads - 1) {
                next = buffer.length - 1;
            } else {
                next = closestIndex(buffer, prev + thd_buf_length - 1);
            }
            threads[t] = new Thread(new MyRunnable(lock, u, buffer, prev, next - prev + 1));
            prev = next + 1;
            threads[t].start();
        }

        // wait for threads to finish
        for (int t = 0; t < numThreads; t++) {
            threads[t].join();
        }

        // generate output
        PrintWriter pw = new PrintWriter(new FileWriter(args[2]));
        u.printAll(pw);
        pw.close();
    }

    static class MyRunnable implements Runnable {
        private Object lock;
        private UnionFind union;
        private byte[] buffer;
        private int offset;
        private int length;

        public MyRunnable(Object lo, UnionFind u, byte[] b, int o, int l) {
            lock = lo;
            union = u;
            buffer = b; 
            offset = o;
            length = l;
        }
        public void run() {
            //System.out.println(offset + " " + length + " " + buffer[offset + length - 1]);
            BufferedReader br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(buffer, offset, length)));
            String line = null;
            int v1, v2;
            String[] verts;
            try {
                while ((line = br.readLine()) != null) {
                    verts = line.split("\\s+");
                    v1 = Integer.parseInt(verts[0]);
                    v2 = Integer.parseInt(verts[1]);
                    union.add(v1);
                    union.add(v2);
                    synchronized(lock) {
                        union.union(v1, v2);
                    }
                }
                br.close();
            } catch (Exception e) {
            }
        }
    }
}
