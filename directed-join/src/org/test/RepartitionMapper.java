package org.test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Collection;
import java.util.Scanner;

public class RepartitionMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Multimap<String,String> hashMultimap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit Si = ((FileSplit) context.getInputSplit());
        // get split number of Si
        String i = Si.getPath().getName().split("_split_")[1];

        // retrieve Ri from hdfs
        String pathString_Ri = Si.getPath().getParent() + "/t2_split_" + i;

        Configuration conf = context.getConfiguration();
        Path path = new Path(pathString_Ri);
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream inputStream = fs.open(path);

        // build hashmultimap
        this.hashMultimap = HashMultimap.create();
        Scanner sc = new Scanner(inputStream);
        while (sc.hasNextLine()){
            String[] line = sc.nextLine().split("\t");
            String key = line[0];
            String val = line[1];
            hashMultimap.put(key,val);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] record = value.toString().split("\t");

        String joinKey = record[0];
        String rest = record[1];

        try{
            Collection<String> Ri_values = this.hashMultimap.get(joinKey);
            for(String entry : Ri_values){
                context.write(
                    new Text(joinKey),
                    new Text(rest + "\t" + entry)
                );
            }
        }
        catch(Exception e){
            System.out.println("Exception with value " + value);
            e.printStackTrace();
        }

    }
}
