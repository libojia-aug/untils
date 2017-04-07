package distic_tid;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Berger on 2017/3/23.
 */
public class disReducer extends Reducer<LongWritable, Text, Text, LongWritable> {

    public void reduce(Text _key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

    }
}