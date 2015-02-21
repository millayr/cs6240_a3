/**
 * @author Ryan Millay
 * @author Nikit Waghela
 * CS6240
 * Assignment 3
 */

package airdel.a3;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class A3_Driver {

	public static void main(String[] args) throws Exception {
		// check the input
		verifyArgs(args);
		
		// learn via some input file
		if(args[0].equals("-learn")){
			
			// create a new hadoop job
			Job j = new Job();
			j.setJarByClass(A3_Driver.class);
			j.setJobName("Delay Learner");
			
			// configure the input and output paths
			FileInputFormat.addInputPath(j, new Path(args[1]));
			FileOutputFormat.setOutputPath(j, new Path("model"));
			
			// configure the map and reduce tasks
			j.setMapperClass(DelayLearnerMapper.class);
			j.setReducerClass(DelayLearnerReducer.class);
			
			// set map output
			j.setMapOutputKeyClass(Text.class);
			j.setMapOutputValueClass(IntWritable.class);
			
			// configure the output settings
			j.setOutputKeyClass(Text.class);
			j.setOutputValueClass(FloatWritable.class);
			
			// run it!
			System.exit(j.waitForCompletion(true) ? 0 : 1);
		}
		// try to predict based on some input
		else if(args[0].equals("-predict")) {
			
		}
		// verify your past predictions
		else {
			
		}
	}
	
	
	public static void verifyArgs(String[] args) {
		// let's verify the number of args
		if(args.length == 0 || (!args[0].equals("-learn") && !args[0].equals("-predict") && !args[0].equals("-check"))) {
			System.err.println("Usage: < -learn | -predict | -check >");
			System.exit(-1);
		} else if(args[0].equals("-learn") && args.length != 2) {
			System.out.println("Usage: -learn <input file>");
			System.exit(-1);
		} else if(args[0].equals("-predict") && args.length != 3) {
			System.out.println("Usage: -predict <model file> <airline file>");
			System.exit(-1);
		} else if(args[0].equals("-check") && args.length != 3){
			System.out.println("Usage: -check <prediction file> <verification file>");
			System.exit(-1);
		}
	}
}
