package distic_tid;

import config.parameter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Berger on 2017/3/23.
 */
public class disMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
        String regEx = "(\\([^\\)]+\\))";
        Pattern pattern = Pattern.compile(regEx);
        String vString = ivalue.toString();
        Matcher matcher = pattern.matcher(vString);
        if(matcher.matches()){
            System.out.println(vString);
        }
    }
}
