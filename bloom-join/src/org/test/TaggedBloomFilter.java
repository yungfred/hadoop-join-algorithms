package org.test;

import org.apache.hadoop.util.bloom.BloomFilter;

public class TaggedBloomFilter extends BloomFilter {
    private String tag;

    public TaggedBloomFilter(){
        // only for hadoop, should never be used
        assert false;
    }

    public TaggedBloomFilter(int vectorSize, int nbHash, int hashType, String tag) {
        super(vectorSize, nbHash, hashType);
        this.tag = tag;
    }


    public String getTag() {
        return tag;
    }

}
