package topN.driver;

import webLog.mapper.webLogMapper;
import webLog.reducer.webLogReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by Berger on 2016/12/7.
 */
public class topNDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

//        conf.set("mapreduce.framework.name", "yarn");

        Job job = Job.getInstance(conf, "topN");
        job.setJarByClass(topN.driver.topNDriver.class);
        job.setMapperClass(webLogMapper.class);
        job.setReducerClass(webLogReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (!job.waitForCompletion(true))
            return;
    }

}

