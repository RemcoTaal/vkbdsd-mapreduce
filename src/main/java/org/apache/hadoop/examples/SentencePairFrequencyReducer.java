package org.apache.hadoop.examples;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.codehaus.jettison.mapped.MappedXMLStreamWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class SentencePairFrequencyReducer
        extends Reducer<IntWritable, PairFrequencyWritable, Text, MyMapWritable> {
    private IntWritable result = new IntWritable();



    public void reduce(IntWritable key, Iterable<PairFrequencyWritable> values, Context context
    ) throws IOException, InterruptedException {
        double frequency = 0.0;
        String pair;

        MyMapWritable pairFrequencyMap = new MyMapWritable();

        for (PairFrequencyWritable pairFrequency: values){
            String[] pairFrequencyArray = pairFrequency.toString().split("\t");
            frequency = Double.parseDouble(pairFrequencyArray[0]);
            pair = pairFrequencyArray[1];

            if (!pairFrequencyMap.containsKey(new Text(pair))) {
                pairFrequencyMap.put(new Text(pair), new DoubleWritable(1));

            } else {
                double oldCount = Double.parseDouble(pairFrequencyMap.get(new Text(pair)).toString());
                DoubleWritable newCount = new DoubleWritable(oldCount + 1);
                pairFrequencyMap.put(new Text(pair), newCount);
            }
        }


        context.write(new Text(key.toString()), pairFrequencyMap);
    }
}