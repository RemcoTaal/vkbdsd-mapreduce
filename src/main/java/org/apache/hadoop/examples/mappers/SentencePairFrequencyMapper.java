package org.apache.hadoop.examples.mappers;

import org.apache.hadoop.examples.writables.StringDoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Locale;
import java.util.StringTokenizer;


public class SentencePairFrequencyMapper
        extends Mapper<Object, Text, IntWritable, StringDoubleWritable> {

    private int sentenceCount = 0;

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
        while (itr.hasMoreTokens()) {

            String sentence = itr.nextToken().toLowerCase(Locale.ROOT);
            String filteredSentence;
            filteredSentence = sentence.replaceAll("[^a-zA-Z ]", "*");
            filteredSentence += new String(" ");

            int n = 2;
            for(int i = 0; i < filteredSentence.length() - n + 1; i++){

                String pair = filteredSentence.substring(i, i + n);
                context.write(new IntWritable(sentenceCount), new StringDoubleWritable(pair, 1.0));
            }
            sentenceCount ++;
        }
    }
}