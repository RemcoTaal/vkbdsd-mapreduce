package mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Locale;
import java.util.StringTokenizer;


public class PairFrequencyMapper
        extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());

        // Loop through all words
        while (itr.hasMoreTokens()) {
            String filteredWord;
            String word = itr.nextToken().toLowerCase(Locale.ROOT);

            // Remove all special characters from the word and replace them with *
            filteredWord = word.replaceAll("[^a-zA-Z ]", "*");
            // Add a space at the end of every word to also introduce end of the word in the bigrams
            filteredWord += " ";


            // Write every character bigram from a word to the context with value 1
            for (int i = 0; i < filteredWord.length() - 1; i++) {
                Text bigram = new Text(filteredWord.substring(i, i + 2));
                context.write(bigram, one);
            }
        }
    }
}