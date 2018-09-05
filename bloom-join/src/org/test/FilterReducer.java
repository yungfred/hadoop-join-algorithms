package org.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.bloom.BloomFilter;

import java.io.IOException;

public class FilterReducer extends Reducer<NullWritable, TaggedBloomFilter, NullWritable, NullWritable> {

    private BloomFilter bloomFilter;
    private int counter;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        this.bloomFilter = new BloomFilter(
                conf.getInt("bloomfilter.vectorSize",0),
                conf.getInt("bloomfilter.nbHash",0),
                conf.getInt("bloomfilter.hashType",0)
        );
        this.counter = 0;
    }


    @Override
    public void reduce(NullWritable key, Iterable<TaggedBloomFilter> values, Context context) throws IOException, InterruptedException {
        //create S1 union S2
        for(TaggedBloomFilter taggedBloomFilter : values) {
            this.bloomFilter.or(taggedBloomFilter);
            counter++;
        }
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        assert counter == 2;

        int reducerID = context.getTaskAttemptID().getTaskID().getId();
        Configuration conf = context.getConfiguration();

        Path pathBloomfilter = new Path(conf.get("base.output.path") + "/bloomfilter/t" + reducerID);
        FileSystem fs = pathBloomfilter.getFileSystem(conf);
        FSDataOutputStream dos = fs.create(pathBloomfilter);

        this.bloomFilter.write(dos);
        dos.close();
    }

}
