package org.test;

import com.google.common.collect.HashMultimap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Scanner;

public class Phase3Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Log LOG = LogFactory.getLog(Phase3Mapper.class);
    private HashMultimap<String, String> multimap_HR_filtered;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // populate multimap_HR_filtered with R'
        this.multimap_HR_filtered = HashMultimap.create(14473324,1);

        Configuration conf = context.getConfiguration();
        String pathString = conf.get("base.output.path") + "/R_filtered";
        Path path = new Path(pathString);
        FileSystem fs = path.getFileSystem(conf);

        RemoteIterator<LocatedFileStatus> files = fs.listFiles(path,false);

        while (files.hasNext()){
            LocatedFileStatus fileStatus = files.next();
            Path path_Ri_filtered = fileStatus.getPath();

            FSDataInputStream inputStream_Ri_filtered = fs.open(path_Ri_filtered);
            Scanner scanner = new Scanner(inputStream_Ri_filtered);

            LOG.info("Mapper starts reading file: " + path_Ri_filtered);

            while (scanner.hasNextLine()){
                String[] line = scanner.nextLine().split("\t");
                String key = line[0];
                String val = line[1];

                this.multimap_HR_filtered.put(key,val);
            }
            LOG.info("HashMultiMap has " + this.multimap_HR_filtered.size() + " entries");
            long heapSize = Runtime.getRuntime().totalMemory();
            long heapMaxSize = Runtime.getRuntime().maxMemory();
            LOG.info("heap: alloc = " + heapSize/Math.pow(2,20)  + " MiB, max = " + heapMaxSize/Math.pow(2,20) + " MiB");
            LOG.info("#########");

            inputStream_Ri_filtered.close();
        }
        LOG.info("Finished building HashMultiMap");
    }


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\t");
        String joinKey = line[0];
        String val = line[1];

        for(String val_R_filtered : this.multimap_HR_filtered.get(joinKey)){
            context.write(new Text(joinKey), new Text(val + "\t" + val_R_filtered));
        }
    }

}
