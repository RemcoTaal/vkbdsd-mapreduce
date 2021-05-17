package mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class TotalFrequencyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] stringArray = value.toString().split("\t");
        Text firstCharOfPair = new Text(stringArray[0].substring(0, 1));

        IntWritable frequency = new IntWritable(Integer.parseInt(stringArray[stringArray.length - 1].replaceAll("[^0-9]", "")));

        context.write(firstCharOfPair, frequency);

    }
}