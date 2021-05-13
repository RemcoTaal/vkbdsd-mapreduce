package org.apache.hadoop.examples;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Locale;
import java.util.StringTokenizer;


public class PairProbabilityMapper
        extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private final Text bigram = new Text();

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {

            //String[] characters = itr.nextToken().split("");
            String word = itr.nextToken().toLowerCase(Locale.ROOT);
            String filteredWord = new String();
            filteredWord = word.replaceAll("[^a-zA-Z ]", "*");
            filteredWord += new String(" ");

            //Bidirectional
            int n = 2;
            for(int i = 0; i < filteredWord.length() - n + 1; i++){

                String pair = filteredWord.substring(i, i + n);
                this.bigram.set(pair);
                context.write(this.bigram, one);
            }
        }
    }
}