package org.test;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator {
    public GroupingComparator(){
        //createInstances must be set to true
        super(TextPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TextPair textPair1 = (TextPair)a;
        TextPair textPair2 = (TextPair)b;
        //only compare key (left) values of a TextPair when grouping
        return textPair1.getLeft().compareTo(textPair2.getLeft());
    }
}
