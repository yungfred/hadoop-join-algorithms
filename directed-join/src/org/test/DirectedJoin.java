package org.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class DirectedJoin extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DirectedJoin(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        if(args.length!=2){
            System.out.print("Please specify input and output");
            System.exit(-1);
        }

        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf,"DirectedJoin");
        job.setJarByClass(DirectedJoin.class);

        job.setMapperClass(RepartitionMapper.class);
        job.setNumReduceTasks(0);// map-only task

        // set input and output path
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // only pass t1 to mapper, since t2 is fetched from hdfs manually
        FileInputFormat.setInputPathFilter(job, CustomPathFilter.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // mapper input
        job.setInputFormatClass(TextInputFormat.class);

        // mapper output
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}