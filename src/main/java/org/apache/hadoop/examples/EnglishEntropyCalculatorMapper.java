package org.apache.hadoop.examples;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class EnglishEntropyCalculatorMapper
        extends Mapper<Object, Text, Text, DoubleWritable> {

    private static HashMap<String, Double> probabilityMap = new HashMap();
    private static HashMap<String, HashMap<String, Double>> unclassifiedMap = new HashMap();

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {

        double entropyTotal = 0.0;


        if (context.getInputSplit().toString().contains("Probability")) {
            String[] probabilityArray = value.toString().split("\t");
            String probabilityMapPair = probabilityArray[0];
            Double probabilityMapProbability = Double.parseDouble(probabilityArray[2]);
            probabilityMap.put(probabilityMapPair, probabilityMapProbability);
        }

        if (context.getInputSplit().toString().contains("Unclassified")) {

            String[] sentenceFrequencies = value.toString().split("\t");
            String sentenceNumber = sentenceFrequencies[0];
            String[] pairFrequencies = sentenceFrequencies[1].split(",");
            HashMap<String, Double> pairFrequenciesMap = new HashMap<>();

            for (String pairFrequency : pairFrequencies) {
                String[] pairFrequencyArray = pairFrequency.split(" = ");
                String pair = pairFrequencyArray[0];
                Double frequency = Double.parseDouble(pairFrequencyArray[1]);
                pairFrequenciesMap.put(pair, frequency);
            }

            unclassifiedMap.put(sentenceNumber, pairFrequenciesMap);
        }


        if (probabilityMap.size() == 418){
            for (Map.Entry<String, HashMap<String, Double>> entry : unclassifiedMap.entrySet()) {
                for (Map.Entry<String, Double> hashmapEntry : entry.getValue().entrySet()) {

                    if (probabilityMap.containsKey(hashmapEntry.getKey())) {

                        double probability = probabilityMap.get(hashmapEntry.getKey());
                        double entropy = hashmapEntry.getValue() * probability;
                        entropyTotal += entropy;
                    }
                }


                context.write(new Text(entry.getKey()), new DoubleWritable(entropyTotal));
                entropyTotal = 0;


            }
        }


    }
}

