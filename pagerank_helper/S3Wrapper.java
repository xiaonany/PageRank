package pagerank_helper;

import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.services.s3.transfer.TransferManager;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;

public class S3Wrapper {
	static final int MAX_CRAWL = 1000000;
	static private AmazonS3 s3;

	@SuppressWarnings("deprecation")
	public static void initiate() {
		AWSCredentials credentials = null;
		try {
			credentials = new BasicAWSCredentials("test", "test");
		} catch (Exception e) {
			System.out.println("credentials invalid!");
		}
		s3 = new AmazonS3Client(credentials);
		Region usStandard = Region.getRegion(Regions.US_EAST_1);
		s3.setRegion(usStandard);
	}

	public static int putFile(String bucket_name, String key_name, File file) {
		try {
			s3.putObject(new PutObjectRequest(bucket_name, key_name, file));
		} catch (AmazonServiceException e) {
			e.printStackTrace();
			return 1;
		}
		return 0;
	}

	public static int putFile(String bucket_name, String key_name, String fileContent) {
		try {
			s3.putObject(bucket_name, key_name, fileContent);
		} catch (AmazonServiceException e) {
			e.printStackTrace();
			return 1;
		}
		return 0;
	}

	public static Bucket getBucket(String bucket_name) {
		Bucket named_bucket = null;
		List<Bucket> buckets = s3.listBuckets();
		for (Bucket b : buckets) {
			if (b.getName().equals(bucket_name)) {
				named_bucket = b;
			}
		}
		return named_bucket;
	}

	public static Bucket createBucket(String bucket_name) {
		Bucket b = null;
		if (s3.doesBucketExist(bucket_name)) {
			System.out.format("Bucket %s already exists.\n", bucket_name);
			b = getBucket(bucket_name);
		} else {
			try {
				b = s3.createBucket(bucket_name);
			} catch (AmazonS3Exception e) {
				System.err.println(e.getErrorMessage());
			}
		}
		return b;
	}

	public static boolean deleteBuckt(String bucket_name) {
		System.out.println("Deleting S3 bucket: " + bucket_name);
		try {
			System.out.println(" - removing objects from bucket");
			ObjectListing object_listing = s3.listObjects(bucket_name);
			while (true) {
				for (Iterator<?> iterator = object_listing.getObjectSummaries().iterator(); iterator.hasNext();) {
					S3ObjectSummary summary = (S3ObjectSummary) iterator.next();
					s3.deleteObject(bucket_name, summary.getKey());
				}

				// more object_listing to retrieve?
				if (object_listing.isTruncated()) {
					object_listing = s3.listNextBatchOfObjects(object_listing);
				} else {
					break;
				}
			}
			;

			System.out.println(" - removing versions from bucket");
			VersionListing version_listing = s3.listVersions(new ListVersionsRequest().withBucketName(bucket_name));
			while (true) {
				for (Iterator<?> iterator = version_listing.getVersionSummaries().iterator(); iterator.hasNext();) {
					S3VersionSummary vs = (S3VersionSummary) iterator.next();
					s3.deleteVersion(bucket_name, vs.getKey(), vs.getVersionId());
				}

				if (version_listing.isTruncated()) {
					version_listing = s3.listNextBatchOfVersions(version_listing);
				} else {
					break;
				}
			}

			System.out.println(" OK, bucket ready to delete!");
			s3.deleteBucket(bucket_name);
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			return false;
		}
		System.out.println("Done!");
		return true;
	}

	public static void main(String[] args) {
//		initiate();

	}

	public static void countBucketCount(int i, int[] count, boolean print) {
		ListObjectsV2Result result;
		ListObjectsV2Request req = new ListObjectsV2Request().withBucketName("crawler-storage-0" + i)
				.withEncodingType("url");
		if (print)
			System.out.print("Counting Crawler Bucket " + i + "... ");
		int c = 0;
		int d = 0;
		do {
			result = s3.listObjectsV2(req);
			for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
			

				
				String key = objectSummary.getKey();
				if (key.startsWith("docs/")) {
					c++;
					if (c % 5000 == 0 && print)
						System.out.print("*");
				} else {
					d++;
					if (d % 5000 == 0 && print)
						System.out.print("-");
					

					
				}
				
				
			}

			req.setContinuationToken(result.getNextContinuationToken());
		} while (result.isTruncated());
		if (print)
			System.out.println("\nBucket " + i + " : Number of docs: " + c);

		count[0] += c;
		count[1] += d;
	}
	
	public static void append_file(int i, BufferedWriter bufferedWriter) {
		Boolean print = true;
		ListObjectsV2Result result;
		ListObjectsV2Request req = new ListObjectsV2Request().withBucketName("crawler-storage-0" + i)
				.withEncodingType("url");
		if (print)
			System.out.print("Reading from bucket " + i + "... ");
		int c = 0;
		int d = 0;
		do {
		
			result = s3.listObjectsV2(req);
			for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
			

				
				String key = objectSummary.getKey();
				//if (key.startsWith("linksmd5/")) {
				if (key.startsWith("links/")) {	
					++c;
					////////////////////////////
					/*
					 *    
					 *   _   _                   
					    | | | | ___ _ __ ___     
					    | |_| |/ _ \ '__/ _ \    
					    |  _  |  __/ | |  __/    
					    |_| |_|\___|_|  \___|    
					                             				
					 */
					String bucketName = "crawler-storage-0"+i;
					S3Object obj = s3.getObject(new GetObjectRequest(bucketName, key));
					BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(obj.getObjectContent()));
					
//					InputStream inputStream = new inputStream();
//					BufferedReader bufferedReader;
//					bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

					c++;
					if (c % 500 == 0 && print)
						System.out.println("\nBucket " + i + " : processed linkfile: " + c);
					
					
					String from_url;
					try {
						from_url = bufferedReader.readLine();
						if (from_url != null && !from_url.trim().equals("")) {
							String to_url;
							StringBuilder sb = new StringBuilder();
							while ((to_url = bufferedReader.readLine()) != null) {
							    to_url = to_url.trim();
							    if (!to_url.equals("")) {
							    	sb.append(from_url+" "+to_url+"\n");
							    }
							}
							bufferedWriter.write(sb.toString());
						}
						
						bufferedWriter.flush();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

					
					
					////////////////////////////
					/*
					 *    
					 *   _   _                   
					    | | | | ___ _ __ ___     
					    | |_| |/ _ \ '__/ _ \    
					    |  _  |  __/ | |  __/    
					    |_| |_|\___|_|  \___|    
					                             				
					 */

				} 
				
				
			}

			req.setContinuationToken(result.getNextContinuationToken());
		} while (result.isTruncated());
		if (print)
			System.out.println("\nBucket " + i + " : Number of linkfile: " + c);


	}
	
	
}
