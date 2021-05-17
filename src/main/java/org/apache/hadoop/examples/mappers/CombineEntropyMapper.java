package org.apache.hadoop.examples.mappers;

import org.apache.hadoop.examples.writables.DoubleDoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;


public class CombineEntropyMapper
        extends Mapper<Object, Text, Text, DoubleDoubleWritable> {

    private static HashMap<String, DoubleDoubleWritable> totalMap = new HashMap();
    private static String lastPair = "";

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {


        if (context.getInputSplit().toString().contains("Dutch")) {

            String[] pairEntropyArray = value.toString().split("\t");
            String pair = pairEntropyArray[0];
            Double entropy = Double.parseDouble(pairEntropyArray[1]);
            totalMap.put(pair, new DoubleDoubleWritable(entropy, 0.0));

            if (totalMap.containsKey(pair)) {
                DoubleDoubleWritable doubleWritable = totalMap.get(pair);
                totalMap.put(pair, new DoubleDoubleWritable(entropy, doubleWritable.double2));
                context.write(new Text(pair), totalMap.get(pair));

            } else {
                totalMap.put(pair, new DoubleDoubleWritable(entropy, 0.0));
            }
        }

        if (context.getInputSplit().toString().contains("English")) {
            String[] pairEntropyArray = value.toString().split("\t");
            String pair = pairEntropyArray[0];
            Double entropy = Double.parseDouble(pairEntropyArray[1]);


            if (totalMap.containsKey(pair)) {
                DoubleDoubleWritable doubleWritable = totalMap.get(pair);
                totalMap.put(pair, new DoubleDoubleWritable(doubleWritable.double1, entropy));
                context.write(new Text(pair), totalMap.get(pair));
            } else {
                totalMap.put(pair, new DoubleDoubleWritable(0.0, entropy));
            }
        }

    }
}

