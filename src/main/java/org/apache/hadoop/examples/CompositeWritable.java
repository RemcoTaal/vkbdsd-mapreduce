package org.apache.hadoop.examples;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeWritable implements Writable {
    double frequency = 0.0;
    double probability = 0.0;


    public CompositeWritable() {
    }

    public CompositeWritable(double frequency, double probability) {
        this.frequency = frequency;
        this.probability = probability;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        frequency = in.readDouble();
        probability = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(frequency);
        out.writeDouble(probability);
    }

    public void merge(CompositeWritable other) {
        this.frequency = other.frequency;
        this.probability = other.probability;
    }

    @Override
    public String toString() {
        return this.frequency + "\t" + this.probability;
    }
}