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
            String[] strings = vString.split("\t");
            System.out.println(strings[0]);
            if(strings.length>1){

                context.write(new LongWritable(Integer.parseInt(strings[1])), new Text(strings[0]));
            }
        }
    }
