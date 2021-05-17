package org.apache.hadoop.examples.reducers;

import org.apache.hadoop.examples.writables.DoubleDoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CombinedEntropyReducer extends Reducer<Text, DoubleDoubleWritable, Text, DoubleDoubleWritable> {

    public void reduce(Text key, Iterable<DoubleDoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        values.forEach(it -> {
            // for every DoubleDoubleWritable value when both double1 and double 2 are filled write it to the context.
            if (it.double1 != 0.0 && it.double2 != 0.0) {
                try {
                    context.write(key, it);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }

        });
    }
}