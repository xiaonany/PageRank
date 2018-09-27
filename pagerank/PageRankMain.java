package pagerank;


/*Pagerank with Apache Spark
 * 
 * Cis555 Group 23
 * Written by Yiyang Zhao
 * 
 * Usage: 
 * input_filename, output_filename, [iteration=15, alpha = 0.85]
 * 
 * input: text file; format: from_url[space]to_url
 * output: textfile, aggregate to 1; format: url[space]pagerank
 * 
 * self-loop removed
 * dangle link fixed
 * 
 * 
 * ### Need Java 8
 */




import org.apache.spark.*;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import com.google.common.collect.Iterables;

import static spark.Spark.*;

import java.util.ArrayList;

import java.util.List;
import java.util.Iterator;
import scala.Tuple2;


public class PageRankMain {

	static int max_iter = 15;
	static String input_path = "";
	static String output_path = "";
	static double cov_alpha = 0.85;
	static double cov_beta = 0.15;
	
	public static void main(String[] args) {
		
		//Read command line args
		
		/*
		 *                              		                             
		      ____             __    
		     / ___|___  _ __  / _|   
		    | |   / _ \| '_ \| |_    
		    | |__| (_) | | | |  _|   
		     \____\___/|_| |_|_|     		                                             		
		 */
		
		SparkConf sparkConf = new SparkConf().setAppName("PageRank").setMaster("local[4]").set("spark.executor.memory","10g");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		
		//args: input_filename, output_filename, iteration=15
		if (args.length < 2) {
			System.err.println("G23 Args: input_filename, output_filename, [iteration=15, alpha = 0.85]");
			System.exit(0);
		}
		
		input_path = args[0];
		output_path = args[1];
		
		if (args.length > 2) {
			max_iter = Integer.parseInt(args[2]);
		}
		
		if (args.length > 3) {
			cov_alpha = Double.parseDouble(args[3]);
			cov_beta = 1 - cov_alpha;
		}
		
		//Read input
		JavaRDD<String> raw_input = context.textFile(input_path);
		
		//Parse input
		JavaPairRDD<String, String> all_link_temp = raw_input.mapToPair(new PairFunction<String, String, String>() {
		      @Override
		      public Tuple2<String, String> call(String s) {
		    	  String safety = s + " a b";
		    	  String[] urllink = safety.split(" ");
		        return new Tuple2<String, String>(urllink[0], urllink[1]);
		      }
		    });
		
		//Remove self loop
		all_link_temp = all_link_temp.filter(t -> !(t._1.equals(t._2)));
		
		//Group from_url and put to_url into list; from_url -> List <to_url>
		JavaPairRDD<String,Iterable<String>> fromurl_tourl = all_link_temp.groupByKey().cache();
		
		//Initial weight
		JavaPairRDD<String,Double> fromurl_weight = fromurl_tourl.mapToPair(t -> new Tuple2<String,Double>(t._1,1.0)).cache();
		
		//Empty weight list
		JavaPairRDD<String,Double> empty_weight = fromurl_tourl.mapToPair(t -> new Tuple2<String,Double>(t._1,0.0)).cache();

		
		//Iteratively calculate weight
		for (int i = 0; i<max_iter; i++) {
			
	
			//Merge fromurl_tourl with fromurl_weight
			JavaPairRDD<String, Tuple2<Iterable<String>,Double>> fromurl_tourl_weight = fromurl_tourl.join(fromurl_weight);
			
			//drop fromurl; only need tourl(list) - weight
			JavaRDD <Tuple2<Iterable<String>,Double>> tourl_weight = fromurl_tourl_weight.values();
			
			//Cast vote, with new RDD with tourl - vote
			JavaPairRDD<String, Double> tourl_vote = tourl_weight
					.flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
						@Override
						public Iterator<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> t) {
							int out_link = Iterables.size(t._1);
							double o_weight = t._2;
							double vote_weight = o_weight / out_link;
							ArrayList<Tuple2<String, Double>> outlink_vote = new ArrayList<Tuple2<String, Double>>();
							for (String n : t._1) {
								outlink_vote.add(new Tuple2<String, Double>(n, vote_weight));
							}

							return outlink_vote.iterator();
						}
					});
			
			//Append all to_url with zero weight to avoid being excluded
			tourl_vote = tourl_vote.union(empty_weight);
			
			//Aggregrate vote, generate new weight
			JavaPairRDD<String, Double> tourl_voteweight = tourl_vote.reduceByKey( (a,b)->(a+b));
			
			//Smoothing using alpha and beta and replace old value
			fromurl_weight = tourl_voteweight.mapValues(w -> w*cov_alpha + cov_beta);

		}
		
		//output	    
		JavaRDD<String> output_rdd = fromurl_weight.map(f -> f._1 + " " + f._2);
		output_rdd.coalesce(1).saveAsTextFile(output_path);
		
		//shutdown
		context.stop();
		
	
	}
}
