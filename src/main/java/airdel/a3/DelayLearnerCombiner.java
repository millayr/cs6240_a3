package airdel.a3;

/**
 * DelayLearnerCombiner combines all values for each unique key and emits the merged sum and count
 * 
 * @author Pramod Khare
 * @Created Tue Feb 24 19:35:48 EST 2015
 * @Modified
 */

import java.io.IOException;
import java.text.MessageFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DelayLearnerCombiner extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
            InterruptedException {
        int sum = 0, total = 0;
        String[] arrDel15SumNCount;

        // Aggregate all sums and counts
        for (final Text arrDel15 : values) {
            arrDel15SumNCount = arrDel15.toString().split("\\|");
            try {
                sum += Integer.parseInt(arrDel15SumNCount[0]);
                total += Integer.parseInt(arrDel15SumNCount[1]);
            } catch (final NumberFormatException nfe) {
                System.out.println("Invalid arrDel15SumNCount value - " + arrDel15.toString());
            }
        }

        // Emit this combined sum and count values separated by pipe char
        context.write(
                key,
                new Text(
                        MessageFormat.format("{0}|{1}", 
                                String.valueOf(sum), 
                                String.valueOf(total))));
    }
}
