package mappers;

import writables.StringDoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Locale;
import java.util.StringTokenizer;


public class SentencePairFrequencyMapper extends Mapper<Object, Text, IntWritable, StringDoubleWritable> {

    private int lineCount = 0;

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {

        // Add a new line delimiter to the tokenizer to split on a new line instead of a word
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");

        // Loop through all lines
        while (itr.hasMoreTokens()) {
            String filteredSentence;
            String sentence = itr.nextToken().toLowerCase(Locale.ROOT);

            // Remove all special characters from the word and replace them with *
            filteredSentence = sentence.replaceAll("[^a-zA-Z ]", "*");
            // Add a space at the end of every line to also represent the end of the last word in the sentence in the bigrams
            filteredSentence += " ";

            // Write every lineCount + a map of the bigram and frequency of 1 to the context
            for(int i = 0; i < filteredSentence.length() - 1; i++){
                String pair = filteredSentence.substring(i, i + 2);
                context.write(new IntWritable(lineCount), new StringDoubleWritable(pair, 1.0));
            }
            lineCount++;
        }
    }
}