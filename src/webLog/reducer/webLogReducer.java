package webLog.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Berger on 2016/12/14.
 */
public class webLogReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    public void reduce(Text _key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;

//        System.out.println(_key);

        for (LongWritable value : values) {
            LongWritable t = value;
            sum += Long.parseLong(t.toString());
        }
        context.write(_key, new LongWritable(sum));

    }
}