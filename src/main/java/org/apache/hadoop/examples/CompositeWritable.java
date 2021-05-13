package org.apache.hadoop.examples;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
public class CompositeWritable implements Writable {
    int frequency = 0;
    int total = 0;

    public CompositeWritable() {
    }

    public CompositeWritable(int frequency, int total) {
        this.frequency = frequency;
        this.total = total;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        frequency = in.readInt();
        total = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(frequency);
        out.writeFloat(total);
    }

    public void merge(CompositeWritable other) {
        this.frequency += other.frequency;
        this.total += other.total;
    }

    @Override
    public String toString() {
        return this.frequency + "\t" + this.total;
    }
}