package org.apache.hadoop.examples;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class PairProbabilityReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    String[] totalList = {};
    String[] pairList = {};
    String[] keyValueList = {};


    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
    ) throws IOException, InterruptedException {


        String writable =  key + ":" ;
        ArrayList<String> keyValue = new ArrayList<>();

        values.forEach(value -> keyValue.add(writable.concat(value.toString())));
        System.out.println("2345");
//
//        if (key.getLength() == 1) {
//            totalList.
//        } else {
//            pairList.add(key);
//        }




    }
}