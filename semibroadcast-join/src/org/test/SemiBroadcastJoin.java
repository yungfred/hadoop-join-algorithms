package org.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class SemiBroadcastJoin extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(SemiBroadcastJoin.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SemiBroadcastJoin(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        if(args.length!=3){
            System.out.print("Please specify input and output");
            System.exit(-1);
        }

        Configuration conf = this.getConf();

        conf.set("mapreduce.input.fileinputformat.split.minsize", args[2]);
        conf.set("mapreduce.input.fileinputformat.split.maxsize", args[2]);

        conf.set("mapreduce.map.memory.mb","78430");
        //conf.set("mapreduce.reduce.memory.mb", "16384");

        conf.set("base.input.path", args[0]);
        conf.set("base.output.path", args[1]);

        startJob1(conf);
        startJob2(conf);
        startJob3(conf);

        return 0;
    }


    private void startJob1(Configuration conf) throws Exception {
        Job job1 = Job.getInstance(conf,"SemiBroadcastJoin-Phase1");
        job1.setJarByClass(SemiBroadcastJoin.class);

        job1.setMapperClass(Phase1Mapper.class);
        job1.setReducerClass(Phase1Reducer.class);

        job1.setNumReduceTasks(1);

        // set input and output path
        FileInputFormat.setInputPaths(job1, new Path(conf.get("base.input.path")));
        // only pass t1 to mapper, since t2 is not needed in job1
        FileInputFormat.setInputPathFilter(job1, CustomPathFilter1.class);
        String outputPath = conf.get("base.output.path") + "/L_uk";
        LOG.info("output path: " + outputPath);
        FileOutputFormat.setOutputPath(job1, new Path(outputPath));

        // mapper input + reducer output
        job1.setInputFormatClass(TextInputFormat.class);
        //job1.setOutputFormatClass(NullOutputFormat.class);

        // mapper output
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(NullWritable.class);

        // reducer output
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(NullWritable.class);


        if(!job1.waitForCompletion(true)){
            System.exit(1);
        }
    }

    private void startJob2(Configuration conf) throws Exception {
        Job job2 = Job.getInstance(conf,"SemiBroadcastJoin-Phase2");
        job2.setJarByClass(SemiBroadcastJoin.class);

        job2.setMapperClass(Phase2Mapper.class);
        // map-only job
        job2.setNumReduceTasks(0);

        // set input and output path
        FileInputFormat.setInputPaths(job2, new Path(conf.get("base.input.path")));
        // only pass t2 to mapper, since t1 is fetched from dfs
        FileInputFormat.setInputPathFilter(job2, CustomPathFilter2.class);
        String outputPath = conf.get("base.output.path") + "/R_filtered";
        LOG.info("output path: " + outputPath);
        FileOutputFormat.setOutputPath(job2, new Path(outputPath));

        // input + output format
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        // mapper output
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);


        if(!job2.waitForCompletion(true)){
            System.exit(2);
        }
    }

    private void startJob3(Configuration conf) throws Exception {
        Job job3 = Job.getInstance(conf,"SemiBroadcastJoin-Phase3");
        job3.setJarByClass(SemiBroadcastJoin.class);

        job3.setMapperClass(Phase3Mapper.class);
        // map-only job
        job3.setNumReduceTasks(0);

        // set input and output path
        FileInputFormat.setInputPaths(job3, new Path(conf.get("base.input.path")));
        // only pass t1 to mapper, since R_filtered is fetched from dfs
        FileInputFormat.setInputPathFilter(job3, CustomPathFilter1.class);
        String outputPath = conf.get("base.output.path") + "/output";
        FileOutputFormat.setOutputPath(job3, new Path(outputPath));

        // input + output format
        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);

        // mapper output
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);


        if(!job3.waitForCompletion(true)){
            System.exit(3);
        }
    }
}
