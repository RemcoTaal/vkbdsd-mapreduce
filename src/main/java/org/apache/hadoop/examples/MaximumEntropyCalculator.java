package org.apache.hadoop.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaximumEntropyCalculator {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);

        Path dutchInputDir = new Path("input/inputDutch");
        Path dutchPairFrequencyOutputDir = new Path("output/outputPairFrequencyDutch");
        Path dutchLetterTotalOutputDir = new Path("output/outputLetterTotalDutch");
        Path dutchProbabilityOutputDir = new Path("output/outputProbabilityDutch");

        Path englishInputDir = new Path("input/inputEnglish");
        Path englishPairFrequencyOutputDir = new Path("output/outputPairFrequencyEnglish");
        Path englishLetterTotalOutputDir = new Path("output/outputLetterTotalEnglish");
        Path englishProbabilityOutputDir = new Path("output/outputProbabilityEnglish");

        Path unclassifiedInputDir = new Path("input/inputUnclassified");

        if (hdfs.exists(dutchPairFrequencyOutputDir)) {
            hdfs.delete(dutchPairFrequencyOutputDir, true);
        }

        Job job = Job.getInstance(conf, "dutch pair frequency");
        job.setJarByClass(MaximumEntropyCalculator.class);
        job.setMapperClass(PairFrequencyMapper.class);
        job.setCombinerClass(PairFrequencyReducer.class);
        job.setReducerClass(PairFrequencyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, dutchInputDir);
        FileOutputFormat.setOutputPath(job, dutchPairFrequencyOutputDir);
        job.waitForCompletion(true);

        if (hdfs.exists(dutchLetterTotalOutputDir)) {
            hdfs.delete(dutchLetterTotalOutputDir, true);
        }

        Job job2 = Job.getInstance(conf, "dutch count totals for letter");
        job2.setJarByClass(MaximumEntropyCalculator.class);
        job2.setMapperClass(TotalFrequencyMapper.class);
        job2.setCombinerClass(TotalFrequencyReducer.class);
        job2.setReducerClass(TotalFrequencyReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, dutchPairFrequencyOutputDir);
        FileOutputFormat.setOutputPath(job2, dutchLetterTotalOutputDir);
        job2.waitForCompletion(true);

        if (hdfs.exists(dutchProbabilityOutputDir)) {
            hdfs.delete(dutchProbabilityOutputDir, true);
        }

        // JOB TO CALCULATE THE PROBABILITY BIGRAMS PER TOTAL FIRST LETTER COUNT
        Job job3 = new Job(conf, "dutch calculate probability");
        job3.setJarByClass(MaximumEntropyCalculator.class);
        FileOutputFormat.setOutputPath(job3, dutchProbabilityOutputDir);


        job3.setMapperClass(PairProbabilityMapper.class);
        job3.setReducerClass(PairProbabilityReducer.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(CompositeWritable.class);

        SequenceFileInputFormat.setInputPaths(job3, dutchPairFrequencyOutputDir, dutchLetterTotalOutputDir);
        job3.setInputFormatClass(TextInputFormat.class);

        job3.waitForCompletion(true);

        if (hdfs.exists(englishPairFrequencyOutputDir)) {
            hdfs.delete(englishPairFrequencyOutputDir, true);
        }

        Job job4 = Job.getInstance(conf, "english pair frequency");
        job4.setJarByClass(MaximumEntropyCalculator.class);
        job4.setMapperClass(PairFrequencyMapper.class);
        job4.setCombinerClass(PairFrequencyReducer.class);
        job4.setReducerClass(PairFrequencyReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job4, englishInputDir);
        FileOutputFormat.setOutputPath(job4, englishPairFrequencyOutputDir);
        job4.waitForCompletion(true);

        if (hdfs.exists(englishLetterTotalOutputDir)) {
            hdfs.delete(englishLetterTotalOutputDir, true);
        }

        Job job5 = Job.getInstance(conf, "english count totals for letter");
        job5.setJarByClass(MaximumEntropyCalculator.class);
        job5.setMapperClass(TotalFrequencyMapper.class);
        job5.setCombinerClass(TotalFrequencyReducer.class);
        job5.setReducerClass(TotalFrequencyReducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job5, englishPairFrequencyOutputDir);
        FileOutputFormat.setOutputPath(job5, englishLetterTotalOutputDir);
        job5.waitForCompletion(true);

        if (hdfs.exists(englishProbabilityOutputDir)) {
            hdfs.delete(englishProbabilityOutputDir, true);
        }

        // JOB TO CALCULATE THE PROBABILITY BIGRAMS PER TOTAL FIRST LETTER COUNT
        Job job6 = new Job(conf, "english calculate probability");
        job6.setJarByClass(MaximumEntropyCalculator.class);
        FileOutputFormat.setOutputPath(job6, englishProbabilityOutputDir);


        job6.setMapperClass(PairProbabilityMapper.class);
        job6.setReducerClass(PairProbabilityReducer.class);

        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(CompositeWritable.class);

        SequenceFileInputFormat.setInputPaths(job6, englishPairFrequencyOutputDir, englishLetterTotalOutputDir);
        job6.setInputFormatClass(TextInputFormat.class);

        System.exit(job6.waitForCompletion(true) ? 0 : 1);

        // Hier moet een job komen die ongeclassificeerde text als input neemt daar een probability van berekent en vervolgends de output/probability van nederlandse en engelse texten vergelijkt om tot een classificatie te komen

    }
}