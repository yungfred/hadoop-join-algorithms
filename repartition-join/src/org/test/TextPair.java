package org.test;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextPair implements WritableComparable<TextPair> {
    private Text left;
    private Text right;

    // default constructor needed by hadoop reflection api
    public TextPair(){
        this.left = new Text();
        this.right = new Text();
    }

    TextPair(Text left, Text right){
        this.left = left;
        this.right = right;
    }

    public Text getLeft() {
        return left;
    }

    public Text getRight() {
        return right;
    }

    @Override
    public int compareTo(TextPair o) {
        //primary compared value
        int keyComp = this.left.compareTo(o.left);

        //secondary compared value
        if(keyComp == 0){
            keyComp = this.right.compareTo(o.right);
        }
        return keyComp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        left.write(dataOutput);
        right.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        left.readFields(dataInput);
        right.readFields(dataInput);
    }

    @Override
    public String toString(){
        return "[" + left + "," + right + "]";
    }
}
