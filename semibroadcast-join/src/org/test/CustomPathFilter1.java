package org.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class CustomPathFilter1 extends Configured implements PathFilter {
    private Configuration conf;

    @Override
    public boolean accept(Path path) {
        try {
            if(path.getFileSystem(conf).isDirectory(path)){
                return true;
            }
        } catch (Exception e){
            e.printStackTrace();
            return false;
        }

        if(path.getName().startsWith("t1")){
            return true;
        }
        return false;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
    }
}
