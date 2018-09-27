package pagerank_helper;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Pagerank_helper_thread extends Thread {
	int i = 0;
	String output_path = "";
	Pagerank_helper_thread(int _i, String _output_path){
		this.i = _i;
		this.output_path = _output_path + _i;
	}
	
	public void run() {
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
				
			S3Wrapper.append_file(i, bufferedWriter);

			bufferedWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
