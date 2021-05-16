package org.apache.hadoop.examples;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;


public class LanguageClassifierMapper
        extends Mapper<Object, Text, Text, FrequencyProbabilityWritable> {

    private static HashMap<String, Double> totalMap = new HashMap();
    private static HashMap<String, Double> frequencyMap = new HashMap();


    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
//        MyMapWritable pairFrequencyMap;
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {



        }

        // aa 1 0.5 2 0.7
        // ab 1 0.5 1 0.3

//        String[] array = value.toString().split("\t");
//        String mapKey = array[0];
//        Integer mapValue = Integer.parseInt(array[1]);
//
//        if (mapKey.length() == 1) {
//            totalMap.put(mapKey, Double.parseDouble(mapValue.toString()));
//        } else if (mapKey.length() == 2) {
//            frequencyMap.put(mapKey, Double.parseDouble(mapValue.toString()));
//        }
//
//
//        frequencyMap.forEach((k, v) -> {
//            String firstChar = Character.toString(k.charAt(0));
//            if (totalMap.containsKey(firstChar)) {
//
//                Double totalForFrequency = totalMap.get(firstChar);
//                Double probability = v / totalForFrequency;
//
//                try {
//                    context.write(new Text(k), new CompositeWritable(v, probability));
//                } catch (IOException | InterruptedException e) {
//                    e.printStackTrace();
//                }
//
//            }
//
//        });


    }
}
