package webLog.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import webLog.mapper.webLogMapper;
import webLog.reducer.webLogReducer;
import webLog.mapper.webLogTopNMapper;
import webLog.reducer.webLogTopNReducer;

/**
 * Created by Berger on 2016/12/14.
 */
public class webLogDriver {
    public static int init(String[] args) throws Exception {
        Configuration conf = new Configuration();

//        conf.set("mapreduce.framework.name", "yarn");
        conf.set("mapred.child.java.opts", "-Xmx4096m");
        conf.set("mapreduce.input.fileinputformat.split.minsize", "2147483648");//更改map输入大小为512M（3个block大小）2147483648 //2G
        conf.setFloat("mapreduce.job.reduce.slowstart.completedmaps", 0.60f);
        conf.setInt("yarn.nodemanager.resource.cpu-vcores", 1);//该运行节点上yarn
        conf.setInt("yarn.scheduler.maximum-allocation-vcores", 4);

        Job jobWebLog = null;
        Job jobWebLogTopN = null;

        jobWebLog = Job.getInstance(conf, "webLogDriver");
        jobWebLog.setJarByClass(webLog.driver.webLogDriver.class);
        jobWebLog.setMapperClass(webLogMapper.class);
        jobWebLog.setReducerClass(webLogReducer.class);
        jobWebLog.setMapOutputKeyClass(Text.class);
        jobWebLog.setMapOutputValueClass(LongWritable.class);
        jobWebLog.setOutputKeyClass(Text.class);
        jobWebLog.setOutputValueClass(LongWritable.class);
        FileInputFormat.setInputPaths(jobWebLog, args[0]);
        FileOutputFormat.setOutputPath(jobWebLog, new Path(args[1]));
        ControlledJob jobWebLogControl = new ControlledJob(conf);
        jobWebLogControl.setJob(jobWebLog);

        jobWebLogTopN = Job.getInstance(conf, "webLogTopNDriver");
        jobWebLogTopN.setJarByClass(webLog.driver.webLogDriver.class);
        jobWebLogTopN.setMapperClass(webLogTopNMapper.class);
        jobWebLogTopN.setReducerClass(webLogTopNReducer.class);
        jobWebLogTopN.setMapOutputKeyClass(LongWritable.class);
        jobWebLogTopN.setMapOutputValueClass(Text.class);
        jobWebLogTopN.setOutputKeyClass(Text.class);
        jobWebLogTopN.setOutputValueClass(LongWritable.class);
        FileInputFormat.setInputPaths(jobWebLogTopN, args[1]);
        FileOutputFormat.setOutputPath(jobWebLogTopN, new Path(args[2]));
        ControlledJob jobWebLogTopNControl = new ControlledJob(conf);
        jobWebLogTopNControl.setJob(jobWebLogTopN);

        jobWebLogTopNControl.addDependingJob(jobWebLogControl);

        JobControl jobControl = new JobControl("webLog");
        jobControl.addJob(jobWebLogControl);
        jobControl.addJob(jobWebLogTopNControl);


        MultipleOutputs.addNamedOutput(jobWebLogTopN, "2", TextOutputFormat.class, Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(jobWebLogTopN, "26", TextOutputFormat.class, Text.class, LongWritable.class);
        MultipleOutputs.addNamedOutput(jobWebLogTopN, "72", TextOutputFormat.class, Text.class, LongWritable.class);
//        MultipleOutputs.addNamedOutput(jobWebLogTopN, "26", FileOutputFormat.class, Text.class, LongWritable.class);

        jobControl.run();
        return jobControl.allFinished()?1:0;
    }
    public static void main(String[] args) throws Exception {
        System.exit(init(args));
    }
}
