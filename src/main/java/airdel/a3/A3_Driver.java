/**
 * Main class for Assignment 3 - to run learn, predict and check tasks
 * @author Ryan Millay, Nikit Waghela, Pramod Khare
 * CS6240
 * Assignment 3
 */

package airdel.a3;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import airdel.a3.util.Timer;

public class A3_Driver {

	public static void main(String[] args) throws Exception {
		// check the input
		verifyArgs(args);
		
		Timer t = new Timer().start();
		// learn via some input file
		if(args[0].equals("-learn")){
			
			// build a new hadoop job
			Job j = createLearnerJob(args);
			
			// run it!
			int exit = j.waitForCompletion(true) ? 0 : 1;
			
			System.out.println(t.stop()+"Exit code\t:\t"+exit);
		}
        // try to predict based on some input
        else if (args[0].equals("-predict")) {
            // call prediction function
            new DelayPredicter(args[1], args[2]).predictArrivalDelay();
            System.out.println("Total time to predict\t:\t" + t.stop());
        }
        // verify your past predictions
        else {
            // call prediction checker function
            new DelayChecker(args[1], args[2]).check().printResults();
            System.out.println("Total time to check\t:\t" + t.stop());
        }
	}
	
	
	public static void verifyArgs(String[] args) {
		// let's verify the number of args
		if(args.length == 0 || (!args[0].equals("-learn") && !args[0].equals("-predict") && !args[0].equals("-check"))) {
			System.err.println("Usage: < -learn | -predict | -check >");
			System.exit(-1);
		} else if(args[0].equals("-learn") && args.length != 3) {
			System.out.println("Usage: -learn <input data file> <output directory>");
			System.exit(-1);
		} else if(args[0].equals("-predict") && args.length != 3) {
			System.out.println("Usage: -predict <model file> <airline file>");
			System.exit(-1);
		} else if(args[0].equals("-check") && args.length != 3){
			System.out.println("Usage: -check <prediction file> <verification file>");
			System.exit(-1);
		}
	}
	
	
	/**
	 * Create a learner hadoop job based on the input arguments
	 * @param args
	 * @return A hadoop job
	 * @throws Exception 
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	public static Job createLearnerJob(String[] args) throws Exception {
		try {
			// create a new hadoop job
			Job j = new Job();
			j.setJarByClass(A3_Driver.class);
			j.setJobName("Delay Learner");
			
			// configure the input and output paths
			FileInputFormat.addInputPath(j, new Path(args[1]));
			FileOutputFormat.setOutputPath(j, new Path(args[2]));
			
			// configure the map and reduce tasks
			j.setMapperClass(DelayLearnerMapper.class);
			j.setCombinerClass(DelayLearnerCombiner.class);
			j.setReducerClass(DelayLearnerReducer.class);
			
			// set map output
			j.setMapOutputKeyClass(Text.class);
			j.setMapOutputValueClass(DelayValue.class);
			
			// configure the output settings
			j.setOutputKeyClass(Text.class);
			j.setOutputValueClass(IntWritable.class);
			
			return j;
		} catch(Exception e) {
			System.out.println("Failed to configure Hadoop learning job!");
			throw e;
		}
	}
}
