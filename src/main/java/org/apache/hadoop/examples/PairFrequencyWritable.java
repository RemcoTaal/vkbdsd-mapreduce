package org.apache.hadoop.examples;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairFrequencyWritable implements Writable {
    String pair = "";
    double frequency = 0.0;

    public PairFrequencyWritable() {
    }

    public PairFrequencyWritable(String pair, double frequency) {
        this.pair = pair;
        this.frequency = frequency;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        pair = in.readUTF();
        frequency = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(pair);
        out.writeDouble(frequency);
    }

    public void merge(PairFrequencyWritable other) {
        this.pair = other.pair;
        this.frequency = other.frequency;
    }

    @Override
    public String toString() {
        return this.frequency + "\t" + this.pair;
    }
}