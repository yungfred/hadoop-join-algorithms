package org.test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class RepartitionMapper extends Mapper<LongWritable, Text, TextPair, TextPair> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] record = value.toString().split("\t");
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

        try{
            context.write(
                    new TextPair(new Text(record[0]), new Text(fileName)),
                    new TextPair(new Text(record[1]), new Text(fileName))
            );
        }
        catch(Exception e){
            System.out.println("Exception with value " + value);
            e.printStackTrace();
        }

    }

}
