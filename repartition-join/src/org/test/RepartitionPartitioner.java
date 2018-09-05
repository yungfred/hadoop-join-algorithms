package org.test;

import org.apache.hadoop.mapreduce.Partitioner;

public class RepartitionPartitioner extends Partitioner<TextPair,TextPair> {

    @Override
    public int getPartition(TextPair key, TextPair value, int numReduceTasks) {
        if(numReduceTasks == 0){
            return 0;
        }
        // adapted HashPartitioner
        return (key.getLeft().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
