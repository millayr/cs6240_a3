/**
 * @author Ryan Millay
 * @author Nikit Waghela
 * CS6240
 * Assignment 3
 */

package airdel.a3;

//Import declarations
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import airdel.a3.util.Parser;


/**
 * Mapper parses the data line by line and generates the emits based
 * on the combinations of attributes and learn the features
 * to reducer
 * 
 * Input Key:  Line in file
 * Input Value:  Text content of the line
 * Output Keys:  Array of fields to consider for pattern detection
 * Output Value:  1 if the flight was delayed by 15 min, 0 otherwise
 */
public class DelayLearnerMapper 
extends Mapper<LongWritable, Text, ArrayWritable, IntWritable> {
	
	private Parser parser = new Parser(',');
	
	@Override
	public void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException {
		// Send the value to the parser to put the data in to a map
		parser.parse(value.toString());

		// only process this line if it's valid
		if(parser.isValid()) {
			// Was this flight delayed?
			int isDelayed = parser.getInt("ArrDel15");
			
			// time to start writing to the context object
			context.write(new ArrayWritable(new String[]{parser.getKeyValuePair("OriginAirportID")}), new IntWritable(isDelayed));
			context.write(new ArrayWritable(new String[]{parser.getKeyValuePair("DestAirportID")}), new IntWritable(isDelayed));
			context.write(new ArrayWritable(new String[]{parser.getKeyValuePair("Carrier")}), new IntWritable(isDelayed));
			context.write(new ArrayWritable(new String[]{parser.getKeyValuePair("OriginAirportID"), parser.getText("DestAirportID")}), new IntWritable(isDelayed));
		}	
	}
}
