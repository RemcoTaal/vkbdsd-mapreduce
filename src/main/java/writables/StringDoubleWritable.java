package writables;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StringDoubleWritable implements Writable {
    public String string = "";
    public double doubleVar = 0.0;

    public StringDoubleWritable() {
    }

    public StringDoubleWritable(String string, double doubleVar) {
        this.string = string;
        this.doubleVar = doubleVar;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        string = in.readUTF();
        doubleVar = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(string);
        out.writeDouble(doubleVar);
    }

    public void merge(StringDoubleWritable other) {
        this.string = other.string;
        this.doubleVar = other.doubleVar;
    }

    @Override
    public String toString() {
        return this.doubleVar + "\t" + this.string;
    }
}