package org.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

import java.io.IOException;

public class RepartitionMapper extends Mapper<LongWritable, Text, TextPair, TextPair> {

    private static final Log log = LogFactory.getLog(RepartitionMapper.class);

    private BloomFilter intersectionBloomfilter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        // we can safely assume, that there are only 2 input splits, since there are 2 reducers in job1
        Path bloomFilterR_Path = new Path(conf.get("base.output.path") + "/bloomfilter/t0");
        Path bloomFilterS_Path = new Path(conf.get("base.output.path") + "/bloomfilter/t1");

        FileSystem fs = bloomFilterR_Path.getFileSystem(conf);

        FSDataInputStream inputStreamR = fs.open(bloomFilterR_Path);
        FSDataInputStream inputStreamS = fs.open(bloomFilterS_Path);

        BloomFilter bloomFilterR = new BloomFilter(
                conf.getInt("bloomfilter.vectorSize",0),
                conf.getInt("bloomfilter.nbHash",0),
                conf.getInt("bloomfilter.hashType",0)
        );
        bloomFilterR.readFields(inputStreamR);

        BloomFilter bloomFilterS = new BloomFilter(
                conf.getInt("bloomfilter.vectorSize",0),
                conf.getInt("bloomfilter.nbHash",0),
                conf.getInt("bloomfilter.hashType",0)
        );
        bloomFilterS.readFields(inputStreamS);

        // create an intersection bloomfilter
        bloomFilterR.and(bloomFilterS);
        this.intersectionBloomfilter = bloomFilterR;
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] record = value.toString().split("\t");
        String joinKey = record[0];
        String val = record[1];
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

        try{
            if(this.intersectionBloomfilter.membershipTest(new Key(joinKey.getBytes()))){
                context.write(
                        new TextPair(new Text(joinKey), new Text(fileName)),
                        new TextPair(new Text(val), new Text(fileName))
                );
            }
        }
        catch(Exception e){
            System.out.println("Exception with value " + value);
            e.printStackTrace();
        }

    }

}
