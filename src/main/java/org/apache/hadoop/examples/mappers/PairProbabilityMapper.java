package org.apache.hadoop.examples.mappers;

import org.apache.hadoop.examples.writables.DoubleDoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;


public class PairProbabilityMapper
        extends Mapper<Object, Text, Text, DoubleDoubleWritable> {

    private static HashMap<String, Double> totalMap = new HashMap();
    private static HashMap<String, Double> frequencyMap = new HashMap();

    public static void clearMaps() {
        totalMap.clear();
        frequencyMap.clear();
    }

    public void map(Object key, Text value, Context context) {

        String[] array = value.toString().split("\t");
        String mapKey = array[0];
        double mapValue = Double.parseDouble(array[1]);

        if (mapKey.length() == 1) {
            totalMap.put(mapKey, mapValue);
        } else if (mapKey.length() == 2) {
            frequencyMap.put(mapKey, mapValue);
        }


        frequencyMap.forEach((k, v) -> {
            String firstChar = Character.toString(k.charAt(0));
            if (totalMap.containsKey(firstChar)) {

                double totalForFrequency = totalMap.get(firstChar);
                double probability = v / totalForFrequency;

                try {
                    context.write(new Text(k), new DoubleDoubleWritable(v, probability));
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }

            }

        });
        
    }
}