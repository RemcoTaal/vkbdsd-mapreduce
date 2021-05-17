package org.apache.hadoop.examples.mappers;

import org.apache.hadoop.examples.writables.DoubleDoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;


public class CombineEntropyMapper extends Mapper<Object, Text, Text, DoubleDoubleWritable> {

    private static HashMap<String, DoubleDoubleWritable> combinedEntropyMap = new HashMap();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        // Check if the key and value come from the dutch input file
        if (context.getInputSplit().toString().contains("Dutch")) {

            String[] pairEntropyArray = value.toString().split("\t");
            String pair = pairEntropyArray[0];
            double dutchEntropy = Double.parseDouble(pairEntropyArray[1]);
            combinedEntropyMap.put(pair, new DoubleDoubleWritable(dutchEntropy, 0.0));

            // If the map contains a record with the key "pair"
            if (combinedEntropyMap.containsKey(pair)) {
                // Get the value of the record
                DoubleDoubleWritable doubleWritable = combinedEntropyMap.get(pair);
                // Add the dutch entropy to the record that already contains the english entropy (double2)
                combinedEntropyMap.put(pair, new DoubleDoubleWritable(dutchEntropy, doubleWritable.double2));
                // Write the key value to the context
                context.write(new Text(pair), combinedEntropyMap.get(pair));

            // If the total map does not contain a record with the key "pair", add a new record to the map
            } else {
                combinedEntropyMap.put(pair, new DoubleDoubleWritable(dutchEntropy, 0.0));
            }
        }

        // Check if the key and value come from the english input file
        if (context.getInputSplit().toString().contains("English")) {
            String[] pairEntropyArray = value.toString().split("\t");
            String pair = pairEntropyArray[0];
            double englishEntropy = Double.parseDouble(pairEntropyArray[1]);

            // if the map contains a record with the key "pair"
            if (combinedEntropyMap.containsKey(pair)) {
                // Get the value of the record
                DoubleDoubleWritable doubleWritable = combinedEntropyMap.get(pair);
                // Add the dutch entropy to the record that already contains the english entropy (double2)
                combinedEntropyMap.put(pair, new DoubleDoubleWritable(doubleWritable.double1, englishEntropy));
                // Write the key value to the context
                context.write(new Text(pair), combinedEntropyMap.get(pair));

            // If the total map does not contain a record with the key "pair", add a new record to the map
            } else {
                combinedEntropyMap.put(pair, new DoubleDoubleWritable(0.0, englishEntropy));
            }
        }

    }
}

