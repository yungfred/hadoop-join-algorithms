package org.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Phase1Mapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private static final Log LOG = LogFactory.getLog(Phase1Mapper.class);
    private Map<String, String> map_Li_uk;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.map_Li_uk = new HashMap<>();
    }


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        String joinKey = line[0];

        if(this.map_Li_uk.get(joinKey) == null){
            this.map_Li_uk.put(joinKey,joinKey);
            context.write(new Text(joinKey),NullWritable.get());
        }
    }

}
