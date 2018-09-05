package org.test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
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

import java.io.IOException;
import java.util.Scanner;

public class BroadcastMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Log LOG = LogFactory.getLog(BroadcastMapper.class);
    private Multimap<String, String> multimap_HR;
    private Multimap<String, String> multimap_HSi;
    private FileSystem fs_input;
    private Path path_R;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // get access to R
        // we assume that S > R (t1 > t2)

        // compare Si with R to decide which hash table to build
        Configuration conf = context.getConfiguration();
        FileSplit inputSplit_Si = ((FileSplit) context.getInputSplit());

        String pathString_R = inputSplit_Si.getPath().getParent().getName() + "/t2.txt";
        this.fs_input = inputSplit_Si.getPath().getFileSystem(conf);
        this.path_R = new Path(pathString_R);

        long size_Si = inputSplit_Si.getLength();
        long size_R = this.fs_input.getFileStatus(this.path_R).getLen();

        if(size_Si > size_R){
            // create and populate HR
            LOG.info("size_Si > sizeR");
            this.multimap_HR = HashMultimap.create();
            populateHR(this.path_R);

        } else {
            // create HSi
            LOG.info("size_Si <= sizeR");
            this.multimap_HSi = HashMultimap.create();
        }
    }


    private void populateHR(Path path_R) throws IOException{
        FSDataInputStream inputStream_R = this.fs_input.open(path_R);
        Scanner scanner_R = new Scanner(inputStream_R);

        while (scanner_R.hasNextLine()){
            String[] line = scanner_R.nextLine().split("\t");
            String key = line[0];
            String val = line[1];

            this.multimap_HR.put(key,val);
        }
        inputStream_R.close();
    }


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] line = value.toString().split("\t");
        String joinKey = line[0];
        String val_Si = line[1];

        if(this.multimap_HR != null){ // ==  size_Si > size_R
            // join each record of Si with entries in multimap HR
            for(String val_R : this.multimap_HR.get(joinKey)){
                context.write(
                    new Text(joinKey), new Text(val_Si + "\t" + val_R)
                );
            }
        } else {
            // populate HSi
            this.multimap_HSi.put(joinKey,val_Si);
        }
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if(this.multimap_HR == null){ // == size_Si < size_R
            // join each record of R with entries in multimap HSi
            FSDataInputStream inputStream_R = this.fs_input.open(this.path_R);
            Scanner scanner_R = new Scanner(inputStream_R);

            while (scanner_R.hasNextLine()){
                String[] line = scanner_R.nextLine().split("\t");
                String joinKey = line[0];
                String val_R = line[1];

                for(String val_Si : this.multimap_HSi.get(joinKey)){
                    context.write(
                            new Text(joinKey), new Text(val_Si + "\t" + val_R)
                    );
                }
            }
            inputStream_R.close();
        }
    }
}
