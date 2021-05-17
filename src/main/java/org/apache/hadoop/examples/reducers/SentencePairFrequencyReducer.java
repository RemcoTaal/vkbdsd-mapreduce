package org.apache.hadoop.examples.reducers;

import org.apache.hadoop.examples.writables.MyMapWritable;
import org.apache.hadoop.examples.writables.StringDoubleWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SentencePairFrequencyReducer extends Reducer<IntWritable, StringDoubleWritable, Text, MyMapWritable> {

    public void reduce(IntWritable key, Iterable<StringDoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        String pair;
        MyMapWritable pairFrequencyMap = new MyMapWritable();

        // For every value in values
        for (StringDoubleWritable pairFrequency : values) {
            String[] pairFrequencyArray = pairFrequency.toString().split("\t");
            pair = pairFrequencyArray[1];

            // If the pairfrequencyMap does not contain a record with the key "pair" add a new record with k:pair ,v:1
            if (!pairFrequencyMap.containsKey(new Text(pair))) {
                pairFrequencyMap.put(new Text(pair), new DoubleWritable(1));

            // If a records exists with the key "pair" increase the value of the record with 1
            } else {
                double oldCount = Double.parseDouble(pairFrequencyMap.get(new Text(pair)).toString());
                DoubleWritable newCount = new DoubleWritable(oldCount + 1);
                pairFrequencyMap.put(new Text(pair), newCount);
            }
        }


        context.write(new Text(key.toString()), pairFrequencyMap);
    }
}