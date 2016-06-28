package ece454;

import java.util.*;
import java.util.concurrent.*;
import java.io.*;

public class CC {
    static class Edge {
	int v1, v2;
	Edge(int v1, int v2) { this.v1 = v1; this.v2 = v2; }
    }

    public static void main(String[] args) throws Exception {
	int numThreads = Integer.parseInt(args[0]);
	int maxVertex = 0;

	// read graph from input file
	List<Edge> edges = new ArrayList<>();
	Set<Integer> vertices = new HashSet<>();
	FileInputStream fis = new FileInputStream(args[1]);
	BufferedReader br = new BufferedReader(new InputStreamReader(fis));
	String line = null;
	while ((line = br.readLine()) != null) {
	    String[] verts = line.split("\\s+");
	    int v1 = Integer.parseInt(verts[0]);
	    int v2 = Integer.parseInt(verts[1]);
	    Edge e = new Edge(v1, v2);
	    maxVertex = Math.max(maxVertex, v1);
	    maxVertex = Math.max(maxVertex, v2);
	    edges.add(e);
	    vertices.add(v1);
	    vertices.add(v2);
	}
	br.close();

	// start worker threads
	Thread[] threads = new Thread[numThreads];
	for (int t = 0; t < numThreads; t++) {
	    threads[t] = new Thread(new MyRunnable());
	    threads[t].start();
	}

	// wait for threads to finish
	for (int t = 0; t < numThreads; t++) {
	    threads[t].join();
	}

	// generate output
	PrintWriter pw = new PrintWriter(new FileWriter(args[2]));
	for (int i: vertices) {
	    pw.println(i + " " + "<replace with component label>");
	}
	pw.close();
    }

    static class MyRunnable implements Runnable {
	public void run() {
	    // add code here
	}
    }
}
