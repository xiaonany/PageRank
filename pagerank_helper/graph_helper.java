package pagerank_helper;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;

public class graph_helper {

	
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
		if (args.length < 1) {
			System.err.println("G23 Args: output_filename");
			System.exit(0);
		}
		
		String output_path = args[0];
		
		
		//create a file writer
		BufferedWriter bufferedWriter ;
		try {
			bufferedWriter  = new BufferedWriter ( new FileWriter ( output_path ) );
			/*
			 *                              
			                             
			    _____           _        
			   | ____|__ _  ___| |__     
			   |  _| / _` |/ __| '_ \    
			   | |__| (_| | (__| | | |   
			   |_____\__,_|\___|_| |_|   
			                             
			                             		
			 */
			S3Wrapper.initiate();
			for (int i = 0; i<10;i++) {
				S3Wrapper.append_file(i, bufferedWriter);
			}
			
			
			bufferedWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		


		
	}
	
}
