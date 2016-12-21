package topN.mapper;

import config.parameter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Berger on 2016/12/7.
 */
public class topNMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
            String vString = ivalue.toString();
            String[] strings = vString.split(parameter.TAB);
            if(strings.length>2){
                context.write(new LongWritable(Integer.parseInt(strings[2])), new Text(strings[0]+parameter.TAB+strings[1]));
            }
        }
    }
