package org.apache.hadoop.examples.mappers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class DutchEntropyCalculatorMapper
        extends Mapper<Object, Text, Text, DoubleWritable> {

    private static HashMap<String, Double> probabilityMap = new HashMap();
    private static HashMap<String, HashMap<String, Double>> unclassifiedMap = new HashMap();

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {

        double entropyTotal = 0.0;


        // Check if the values come from the probability file and put the bigram and probability in a map
        if (context.getInputSplit().toString().contains("Probability")) {
            String[] probabilityArray = value.toString().split("\t");
            String probabilityMapBigram = probabilityArray[0];
            Double probabilityMapProbability = Double.parseDouble(probabilityArray[2]);
            probabilityMap.put(probabilityMapBigram, probabilityMapProbability);
        }

        // Check if the values come from the unclassified sentencerow / bigramfrequencies file
        if (context.getInputSplit().toString().contains("Unclassified")) {
            String[] sentenceFrequencies = value.toString().split("\t");
            String sentenceNumber = sentenceFrequencies[0];
            String[] bigramFrequencies = sentenceFrequencies[1].split(",");
            HashMap<String, Double> bigramFrequenciesMap = new HashMap<>();

            // create a new map which contains all the bigrams + frequencies for a certain row
            for (String bigramFrequency : bigramFrequencies) {
                String[] bigramFrequencyArray = bigramFrequency.split(" = ");
                String bigram = bigramFrequencyArray[0];
                Double frequency = Double.parseDouble(bigramFrequencyArray[1]);
                bigramFrequenciesMap.put(bigram, frequency);
            }

            // add to the map the rownumber of the sentence and map with all bigramfrequencies for that row
            unclassifiedMap.put(sentenceNumber, bigramFrequenciesMap);
        }

        // If the mapsize equals the correct size for the dutch probability
        if (probabilityMap.size() == 641) {

            
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

