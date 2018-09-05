package org.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class RepartitionJoin extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new RepartitionJoin(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        if(args.length!=3){
            System.out.println("Please specify all arguments");
            System.exit(-1);
        }

        Configuration conf = this.getConf();
        conf.set("mapreduce.input.fileinputformat.split.minsize", args[2]);
        conf.set("mapreduce.input.fileinputformat.split.maxsize", args[2]);

        Job job = Job.getInstance(conf,"RepartitionJoin");
        job.setJarByClass(RepartitionJoin.class);

        job.setNumReduceTasks(5);

        job.setMapperClass(RepartitionMapper.class);
        job.setReducerClass(RepartitionReducer.class);
        job.setPartitionerClass(RepartitionPartitioner.class);
        job.setGroupingComparatorClass(GroupingComparator.class);

        // set input and output path
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // mapper input + reducer output
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //job.setCombinerClass(...);

        // mapper output
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(TextPair.class);

        // reducer output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}