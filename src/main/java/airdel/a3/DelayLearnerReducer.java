package airdel.a3;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DelayLearnerReducer

extends Reducer<ArrayWritable, IntWritable, Text, FloatWritable> {
	
	/**
	 * Learns the features and puts down the learnings in
	 * model
	 * @author nikit
	 * 
	 * produces <ArrayWritable, IntWritable>
	 */
	@Override
	public void reduce(ArrayWritable key, Iterable<IntWritable> values,
		Context context) throws IOException, InterruptedException {
		int sum = 0;
		int total = 0;
		for(IntWritable arrDel15: values) {
			sum += arrDel15.get();
			total++;
		}
		float delPercentage = 0.0f;
		if(total > 0) 
			delPercentage = (float) sum/total;
		
		context.write (
				new Text(StringUtils.join(key.toStrings(), ",")), 
				new FloatWritable(delPercentage)
			);
	}
}
