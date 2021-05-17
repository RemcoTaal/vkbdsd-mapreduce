package org.apache.hadoop.examples;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PairProbabilityReducer
        extends Reducer<Text, DoubleDoubleWritable, Text, DoubleDoubleWritable> {


    public void reduce(Text key, Iterable<DoubleDoubleWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        DoubleDoubleWritable out = new DoubleDoubleWritable();

        for (DoubleDoubleWritable next : values)
        {
            out.merge(next);
        }

        context.write(key, out);
    }
}