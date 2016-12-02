package game360Classification.mapper;

/**
 * Created by Berger on 2016/12/2.
 */


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class game360ClassificationMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    LongWritable one = new LongWritable(1);

    public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
        String vString = ivalue.toString();
        String[] strings = vString.split(" ");
        if (strings.length > 3) {
            for (int i = 5; i < strings.length; i++) {
                context.write(new Text(strings[i]), one);
            }
        }
    }
}
