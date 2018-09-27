package pagerank_helper;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.io.BufferedReader;

public class Graph2 {

	
	public static void main(String[] args) {
	
		/*
		 *                              
		  ___                   _    
		 |_ _|_ __  _ __  _   _| |_  
		  | || '_ \| '_ \| | | | __| 
		  | || | | | |_) | |_| | |_  
		 |___|_| |_| .__/ \__,_|\__| 
		           |_|               
		                             		
		 */
		
		ArrayList<Pagerank_helper_thread> threads = new ArrayList<Pagerank_helper_thread>();
		
		if (args.length < 1) {
			System.err.println("G23 Args: output_filename");
			System.exit(0);
		}
		
		String output_path = args[0];
			for (int i = 0; i<10;i++) {
				Pagerank_helper_thread athread = new Pagerank_helper_thread(i,output_path);
				threads.add(athread);
				athread.start();
				
			}
		
	}
	
}
