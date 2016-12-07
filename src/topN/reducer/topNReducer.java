package topN.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Berger on 2016/12/7.
 */
public class topNReducer extends Reducer<LongWritable, Text, Text, LongWritable> {

        public void reduce(Text _key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            for (LongWritable value : values) {
                context.write(_key, value);
            }
        }
    }