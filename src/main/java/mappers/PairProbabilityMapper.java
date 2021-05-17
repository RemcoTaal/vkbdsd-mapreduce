package mappers;

import writables.DoubleDoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;


public class PairProbabilityMapper extends Mapper<Object, Text, Text, DoubleDoubleWritable> {

    // Contains the key letter and total amount for the letter
    private static HashMap<String, Double> totalMap = new HashMap();
    // Contains the bigram and the frequency of the bigram
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


        // For each value in the frequencyMap write a record with bigram, frequency probability
        frequencyMap.forEach((bigram, frequency) -> {
            String firstChar = Character.toString(bigram.charAt(0));

            if (totalMap.containsKey(firstChar)) {
                double totalForLetter = totalMap.get(firstChar);
                double probability = frequency / totalForLetter;

                try {
                    context.write(new Text(bigram), new DoubleDoubleWritable(frequency, probability));
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }

        });
        
    }
}