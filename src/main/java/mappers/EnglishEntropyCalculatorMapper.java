package mappers;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class EnglishEntropyCalculatorMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    // Contains <bigram, probability>
    private static HashMap<String, Double> probabilityMap = new HashMap();
    // Contains <rownumber of inputText, <bigram, probability>>
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

            // Create a new map which contains all the bigrams + frequencies for a certain row
            for (String bigramFrequency : bigramFrequencies) {
                String[] bigramFrequencyArray = bigramFrequency.split(" = ");
                String bigram = bigramFrequencyArray[0];
                Double frequency = Double.parseDouble(bigramFrequencyArray[1]);
                bigramFrequenciesMap.put(bigram, frequency);
            }

            // add to the map the rownumber of the sentence and map with all bigramfrequencies for that row
            unclassifiedMap.put(sentenceNumber, bigramFrequenciesMap);
        }


        // If the mapsize equals the correct size according to the english probability
        if (probabilityMap.size() == 418) {

            // For every row in the unclassified map
            for (Map.Entry<String, HashMap<String, Double>> record : unclassifiedMap.entrySet()) {
                // For every record in the bigram, frequency map
                for (Map.Entry<String, Double> bigramFrequencyRecord : record.getValue().entrySet()) {

                    // If the probabilityMap contains a record with the key "bigram" calculate the entropy for the row
                    if (probabilityMap.containsKey(bigramFrequencyRecord.getKey())) {
                        double probability = probabilityMap.get(bigramFrequencyRecord.getKey());
                        double entropy = bigramFrequencyRecord.getValue() * probability;
                        entropyTotal += entropy;
                    }
                }

                // Write the row number of inputText and the dutch entropy value
                context.write(new Text(record.getKey()), new DoubleWritable(entropyTotal));

                // Reset the entropy value for the next row
                entropyTotal = 0;

            }
        }

    }
}

