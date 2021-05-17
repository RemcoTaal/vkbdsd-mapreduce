package org.apache.hadoop.examples.mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class LanguageClassifierMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {


    private final static IntWritable one = new IntWritable(1);
    

    public void map(LongWritable key, Text value, Context context
    ) throws IOException, InterruptedException {

        String[] pairEntropyArray = value.toString().split("\t");
        double dutchEntropy = Double.parseDouble(pairEntropyArray[1]);
        double englishEntropy = Double.parseDouble(pairEntropyArray[2]);

        if (dutchEntropy > englishEntropy) {
            context.write(new Text("NEDERLANDS"), one);
        } else {
            context.write(new Text("ENGELS"), one);
        }

    }
}
