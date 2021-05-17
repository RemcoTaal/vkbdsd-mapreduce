package org.apache.hadoop.examples;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CombinedEntropyReducer
        extends Reducer<Text, DoubleDoubleWritable, Text, DoubleDoubleWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<DoubleDoubleWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        values.forEach(it -> {
            if (it.double1 != 0.0 && it.double2 != 0.0) {
                try {
                    context.write(key, it);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        });
    }
}