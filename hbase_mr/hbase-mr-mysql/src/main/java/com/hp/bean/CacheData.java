package com.hp.bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CacheData implements WritableComparable<CacheData> {

    public int compareTo(CacheData o) {
        return 0;
    }

    public void write(DataOutput dataOutput) throws IOException {

    }

    public void readFields(DataInput dataInput) throws IOException {

    }
}
