package org.test;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class FilterPartitioner extends Partitioner<NullWritable,TaggedBloomFilter> {

    @Override
    public int getPartition(NullWritable key, TaggedBloomFilter value, int numReduceTasks) {
        if(numReduceTasks == 0){
            return 0;
        }
        // adapted HashPartitioner
        return (value.getTag().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
