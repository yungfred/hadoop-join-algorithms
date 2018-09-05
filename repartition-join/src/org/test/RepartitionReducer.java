package org.test;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class RepartitionReducer extends Reducer<TextPair, TextPair, Text, Text> {

    public void reduce(TextPair key, Iterable<TextPair> values, Context context) throws IOException, InterruptedException {
        List<Text> buf = new LinkedList<>();

        /*
        for(TextPair v : values) {
            System.out.println("key: " + key + "," + v);
            if (v.getRight().toString().equals("t1.txt")) {
                buf.add(new Text(v.getLeft().toString()));
            }
        }
        */


        // read the first values from table1 into buf
        Iterator<TextPair> iterator = values.iterator();
        if(!iterator.hasNext())
            return;

        TextPair v = iterator.next();
        while(v.getRight().toString().equals("t1.txt")){
            buf.add(new Text(v.getLeft().toString()));

            if(iterator.hasNext()){
                v = iterator.next();
            } else {
                return;
            }
        }

        // build the cross product
        do {
            for(Text v2 : buf){
                context.write(key.getLeft(), new Text(v2 + "\t" + v.getLeft()));
            }

            if(iterator.hasNext()){
                v = iterator.next();
            } else {
                return;
            }
        } while (true);
    }
}
