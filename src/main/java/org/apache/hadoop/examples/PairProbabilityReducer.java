package org.apache.hadoop.examples;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PairProbabilityReducer
        extends Reducer<Text, CompositeWritable, Text, CompositeWritable> {


    public void reduce(Text key, Iterable<CompositeWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        CompositeWritable out = new CompositeWritable();

        for (CompositeWritable next : values)
        {
            out.merge(next);
        }

        context.write(key, out);
    }
}