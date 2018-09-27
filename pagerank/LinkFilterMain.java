package pagerank;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import com.google.common.collect.Iterables;

import scala.Tuple2;

public class LinkFilterMain {
	
	
	static String input_path = "";
	static String output_path = "";
	
	public static void main(String[] args) {
		
		//Read command line args
		
		SparkConf sparkConf = new SparkConf().setAppName("LinkFilter").setMaster("local[4]").set("spark.executor.memory","1g");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		
		//args: input_filename, output_filename, iteration=15
		if (args.length < 2) {
			System.err.println("G23 Filter Args: input_filename, output_filename, [iteration=15, alpha = 0.85]");
			System.exit(0);
		}
		
		input_path = args[0];
		output_path = args[1];
		
		
		//Read input
		JavaRDD<String> raw_input = context.textFile(input_path);
		
		//Parse input
		JavaPairRDD<String, String> all_link_temp = raw_input.mapToPair(new PairFunction<String, String, String>() {
		      @Override
		      public Tuple2<String, String> call(String s) {
		    	  String safety = s + " a b";
		    	  String[] urllink = safety.split(" ");
		    	  
		    	  try {
		    		  urllink[0] = new URL(urllink[0]).getHost();
		    		  urllink[1] = new URL(urllink[1]).getHost();
		    	  } catch (MalformedURLException e) {
		    		  
		    	  }

		        return new Tuple2<String, String>(urllink[0], urllink[1]);
		      }
		    });
		
		//filter out intra-site link
		all_link_temp = all_link_temp.filter(t -> !(t._1.equals(t._2)));
		

		//output	    
		JavaRDD<String> output_rdd = all_link_temp.map(f -> f._1 + " " + f._2);
		output_rdd.coalesce(1).saveAsTextFile(output_path);
		
		//shutdown
		context.stop();
		
	
	}
}
