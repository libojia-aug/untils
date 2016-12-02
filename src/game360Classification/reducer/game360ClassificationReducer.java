package game360Classification.reducer;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class game360ClassificationReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    public void reduce(Text _key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        for (LongWritable value : values) {
            LongWritable t = value;
            sum += Long.parseLong(t.toString());
        }
        if (sum > 10) {
            context.write(_key, new LongWritable(sum));
        }
    }
}