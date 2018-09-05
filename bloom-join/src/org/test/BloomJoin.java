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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class BloomJoin extends Configured implements Tool {

    private static final Log log = LogFactory.getLog(BloomJoin.class);


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new BloomJoin(), args);
        System.exit(res);
    }


    public int run(String[] args) throws Exception {
        if(args.length!=3){
            System.out.print("Please specify input and output");
            System.exit(-1);
        }

        Configuration conf = this.getConf();

        conf.set("base.input.path", args[0]);
        conf.set("base.output.path", args[1]);

        conf.set("mapreduce.input.fileinputformat.split.minsize", args[2]);
        conf.set("mapreduce.input.fileinputformat.split.maxsize", args[2]);

        // https://krisives.github.io/bloom-calculator/
        // n = 1437758757, p = 0.1

        conf.setInt("bloomfilter.vectorSize", 81472997); // old: 256
        conf.setInt("bloomfilter.nbHash", 3);
        conf.setInt("bloomfilter.hashType", 1); // 0: JenkinsHash, 1: MurmurHash

        log.info("\n#####################\nStarting job 1\n#####################");
        startJob1(conf);
        log.info("\n#####################\nStarting job 2\n#####################");
        startJob2(conf);

        return 0;
    }


    private void startJob1(Configuration conf) throws Exception {
        Job job = Job.getInstance(conf,"BloomJoin");
        job.setJarByClass(BloomJoin.class);

        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(FilterReducer.class);
        // number of input files
        job.setNumReduceTasks(2);

        FileInputFormat.setInputPaths(job, new Path(conf.get("base.input.path")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("base.output.path") + "/bloomfilter"));

        job.setPartitionerClass(FilterPartitioner.class);

        // mapper output
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(TaggedBloomFilter.class);

        // reducer output
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(TaggedBloomFilter.class);

        if(!job.waitForCompletion(true)){
            System.exit(1);
        }
    }


    private void startJob2(Configuration conf) throws Exception {
        Job job = Job.getInstance(conf,"BloomJoin");
        job.setJarByClass(BloomJoin.class);

        job.setMapperClass(RepartitionMapper.class);
        job.setReducerClass(RepartitionReducer.class);
        // number of partitions, can be arbitrarily set
        job.setNumReduceTasks(5);

        job.setPartitionerClass(RepartitionPartitioner.class);
        job.setGroupingComparatorClass(GroupingComparator.class);

        // set input and output path
        FileInputFormat.setInputPaths(job, new Path(conf.get("base.input.path")));
        FileOutputFormat.setOutputPath(job, new Path(conf.get("base.output.path") + "/final"));

        // mapper output
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(TextPair.class);

        // reducer output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if(!job.waitForCompletion(true)){
            System.exit(2);
        }
    }

}