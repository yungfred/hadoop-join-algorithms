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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class Phase2Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Log LOG = LogFactory.getLog(Phase2Mapper.class);
    private Map<String, String> map_L_uk;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.map_L_uk = new HashMap<>();

        // load L_uk into memory
        Configuration conf = context.getConfiguration();

        // safe assumption that only a single file exists, since phase 1 only has 1 reducer
        String pathString_L_uk = conf.get("base.output.path") + "/L_uk/part-r-00000";
        Path path_L_uk = new Path(pathString_L_uk);
        FileSystem fs = path_L_uk.getFileSystem(conf);

        FSDataInputStream inputStream_L_uk = fs.open(path_L_uk);
        Scanner scanner = new Scanner(inputStream_L_uk);

        while (scanner.hasNextLine()){
            String[] line = scanner.nextLine().split("\t");
            String key = line[0];

            this.map_L_uk.put(key,key);
        }
        inputStream_L_uk.close();
    }


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        String joinKey = line[0];
        String val = line[1];

        if(this.map_L_uk.get(joinKey) != null){
            context.write(new Text(joinKey),new Text(val));
        }
    }

}
