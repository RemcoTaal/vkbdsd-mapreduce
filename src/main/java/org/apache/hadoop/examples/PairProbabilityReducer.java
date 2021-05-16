package org.apache.hadoop.examples;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PairProbabilityReducer
        extends Reducer<Text, FrequencyProbabilityWritable, Text, FrequencyProbabilityWritable> {


    public void reduce(Text key, Iterable<FrequencyProbabilityWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        FrequencyProbabilityWritable out = new FrequencyProbabilityWritable();

        for (FrequencyProbabilityWritable next : values)
        {
            out.merge(next);
        }

        context.write(key, out);
    }
}