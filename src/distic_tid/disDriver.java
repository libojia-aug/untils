package distic_tid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by Berger on 2017/3/23.
 */
public class disDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

//        conf.set("mapreduce.framework.name", "yarn");

        Job job = Job.getInstance(conf, "disDriver");
        job.setJarByClass(disDriver.class);
        job.setMapperClass(disMapper.class);
        job.setReducerClass(disReducer.class);

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
