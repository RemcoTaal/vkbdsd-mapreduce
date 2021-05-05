package org.apache.hadoop.examples;

import org.apache.hadoop.mapred.join.TupleWritable;
import sun.reflect.generics.tree.Tree;

import java.util.*;
import java.util.regex.Pattern;

public class Test2 {
    public static List<String> ngrams(int n, String str) {
        List<String> ngrams = new ArrayList<String>();
        for (int i = 0; i < str.length() - n + 1; i++)
            // Add the substring or size n
            ngrams.add(str.substring(i, i + n));
        // In each iteration, the window moves one step forward
        // Hence, each n-gram is added to the list

        return ngrams;
    }

    public static HashMap<String, Integer> countBigrams(List<String> bigrams) {
        HashMap<String, Integer> frequencyMap = new HashMap<>();

        bigrams.forEach(bigram -> {
            int frequency = Collections.frequency(bigrams, bigram);
            frequencyMap.put(bigram, frequency);
        });

        return frequencyMap;
    }

    public static HashMap<String, Integer> countTotalForLetter(HashMap<String, Integer> frequencyMap) {
        HashMap<String, Integer> totalMap = new HashMap<>();
        ArrayList<Character> characters = new ArrayList<>();


        for (Map.Entry<String, Integer> entry : frequencyMap.entrySet()) {
            characters.add(entry.getKey().charAt(0));
        }

        characters.forEach(character -> {
            int frequency = Collections.frequency(characters, character);
            totalMap.put(character.toString(), frequency);
        });

        return totalMap;
    }

    public static HashMap<String, TreeMap> mergeFrequencyWithTotal(HashMap<String, Integer> frequencyMap, HashMap<String, Integer> totalMap) {
        TreeMap<String, Integer> sortedFrequencyMap = new TreeMap<>(frequencyMap);
        TreeMap<String, Integer> sortedTotalMap = new TreeMap<>(totalMap);
        HashMap<String, TreeMap> frequencyTotal = new HashMap<>();

        for (Map.Entry<String, Integer> totalEntry : sortedTotalMap.entrySet()) {
            HashMap<String, Integer> combinationList = new HashMap<>();

            for (Map.Entry<String, Integer> frequencyEntry : sortedFrequencyMap.entrySet()) {

                if (frequencyEntry.getKey().charAt(0) == totalEntry.getKey().charAt(0)) {
                    combinationList.put(frequencyEntry.getKey(), frequencyEntry.getValue());
                }
            }

            TreeMap listFreqTotal = new TreeMap<>();
            listFreqTotal.put("combinations", combinationList);
            listFreqTotal.put("total", totalEntry.getValue());
            frequencyTotal.put(totalEntry.getKey(), listFreqTotal);
        }

        return frequencyTotal;
    }


    public static void main(String args[]) {
        String s = "rabarber".toLowerCase(Locale.ROOT);
        List<String> bigrams = ngrams(2, s);

        HashMap<String, Integer> frequencyMap = countBigrams(bigrams);
        HashMap<String, Integer> totalMap = countTotalForLetter(frequencyMap);
        HashMap<String, TreeMap> mergedMap = mergeFrequencyWithTotal(frequencyMap, totalMap);


//        System.out.println(frequencyMap);
//        System.out.println(totalMap);
        System.out.println(mergeFrequencyWithTotal(frequencyMap, totalMap));


    }

}
