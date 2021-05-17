package org.apache.hadoop.examples.writables;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DoubleDoubleWritable implements Writable {
    public double double1 = 0.0;
    public double double2 = 0.0;


    public DoubleDoubleWritable() {
    }

    public DoubleDoubleWritable(double double1, double double2) {
        this.double1 = double1;
        this.double2 = double2;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        double1 = in.readDouble();
        double2 = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(double1);
        out.writeDouble(double2);
    }

    public void merge(DoubleDoubleWritable other) {
        this.double1 = other.double1;
        this.double2 = other.double2;
    }

    @Override
    public String toString() {
        return this.double1 + "\t" + this.double2;
    }
}