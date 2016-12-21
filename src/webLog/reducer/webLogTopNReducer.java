package webLog.reducer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import config.parameter;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Created by Berger on 2016/12/16.
 */
public class webLogTopNReducer extends Reducer<LongWritable, Text, Text, LongWritable> {

    private MultipleOutputs<Text, LongWritable> mos;
    public void setup(Reducer.Context context) throws IOException, InterruptedException {
        mos = new MultipleOutputs(context);
    }

    public void cleanup(Reducer.Context context) throws IOException, InterruptedException {
        mos.close();
    }

    public void reduce(LongWritable _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String[] keyStrings = value.toString().split(parameter.TAB);
//            switch (keyStrings[0]){
//                case "2": {
//                    mos.write("sourceIP", new Text(keyStrings[1]), _key);
//                    break;
//                }
//                case "26": {
//                    mos.write("apName", new Text(keyStrings[1]), _key);
//                    break;
//                }
//                case "72": {
//                    mos.write("deCity", new Text(keyStrings[1]), _key);
//                    break;
//                }
//            }
            try {
                mos.write(keyStrings[0], new Text(keyStrings[1]), _key);
            }catch (Exception e){

            }

        }
    }
}