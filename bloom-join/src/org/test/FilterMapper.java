package org.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.bloom.Key;

import java.io.IOException;

public class FilterMapper extends Mapper<LongWritable, Text, NullWritable, TaggedBloomFilter> {

    private final Log log = LogFactory.getLog(this.getClass());
    private TaggedBloomFilter taggedBloomFilter;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

        this.taggedBloomFilter = new TaggedBloomFilter(
                conf.getInt("bloomfilter.vectorSize",0),
                conf.getInt("bloomfilter.nbHash",0),
                conf.getInt("bloomfilter.hashType",0),
                fileName
        );
    }


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        String joinKey = line[0];
        //String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

        Key k = new Key(joinKey.getBytes());
        this.taggedBloomFilter.add(k);
    }


    @Override
    public void cleanup(Context context) {
        try {
            context.write(NullWritable.get(), this.taggedBloomFilter);
        } catch (Exception e){
            log.error("Error while writing bloom filter");
            e.printStackTrace();
            System.exit(1);
        }
    }

}
