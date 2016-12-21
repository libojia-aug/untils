package game360Classification.driver;

/**
 * Created by Berger on 2016/12/2.
 */

import game360Classification.mapper.game360ClassificationMapper;
import game360Classification.reducer.game360ClassificationReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class game360ClassificationDriver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

//        conf.set("mapreduce.framework.name", "yarn");

        Job job = Job.getInstance(conf, "classification");
        job.setJarByClass(game360ClassificationDriver.class);
        job.setMapperClass(game360ClassificationMapper.class);
        job.setReducerClass(game360ClassificationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (!job.waitForCompletion(true))
            return;
    }

}

