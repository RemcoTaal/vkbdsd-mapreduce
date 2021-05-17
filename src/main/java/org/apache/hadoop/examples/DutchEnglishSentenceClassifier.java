package org.apache.hadoop.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.mappers.*;
import org.apache.hadoop.examples.reducers.*;
import org.apache.hadoop.examples.writables.DoubleDoubleWritable;
import org.apache.hadoop.examples.writables.StringDoubleWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DutchEnglishSentenceClassifier {

    public static void main(String[] args) throws Exception {

        // Making a configuration and retrieving the filesystem
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);

        // All dutch Paths
        Path dutchInputDir = new Path("input/inputDutch");
        Path dutchPairFrequencyOutputDir = new Path("output/outputPairFrequencyDutch");
        Path dutchLetterTotalOutputDir = new Path("output/outputLetterTotalDutch");
        Path dutchProbabilityOutputDir = new Path("output/outputProbabilityDutch");
        Path dutchEntropyOutputDir = new Path("output/outputEntropyDutch");

        // All english Paths
        Path englishInputDir = new Path("input/inputEnglish");
        Path englishPairFrequencyOutputDir = new Path("output/outputPairFrequencyEnglish");
        Path englishLetterTotalOutputDir = new Path("output/outputLetterTotalEnglish");
        Path englishProbabilityOutputDir = new Path("output/outputProbabilityEnglish");
        Path englishEntropyOutputDir = new Path("output/outputEntropyEnglish");

        // All paths concerning the (un)classified text 
        Path unclassifiedSentenceBigramFrequencyOutputDir = new Path("output/outputSentenceBigramFrequencyUnclassified");
        Path unclassifiedInputDir = new Path("input/inputUnclassified");
        Path classifiedOutputDir = new Path("output/outputClassified");
        Path combinedEntropyOutputDir = new Path("output/combinedEntropy");

        // calculating the probability for dutch text
        calculateProbability(dutchInputDir, dutchPairFrequencyOutputDir, dutchLetterTotalOutputDir, dutchProbabilityOutputDir, conf, hdfs);

        // calculating the probability for english text
        calculateProbability(englishInputDir, englishPairFrequencyOutputDir, englishLetterTotalOutputDir, englishProbabilityOutputDir, conf, hdfs);

        // Job for generating the bigram frequencies of every row in the unclassified text
        if (hdfs.exists(unclassifiedSentenceBigramFrequencyOutputDir)) {
            hdfs.delete(unclassifiedSentenceBigramFrequencyOutputDir, true);
        }

        Job job1 = Job.getInstance(conf, "unclassified sentence bigram frequencies");
        job1.setJarByClass(DutchEnglishSentenceClassifier.class);
        job1.setMapperClass(SentencePairFrequencyMapper.class);
        job1.setReducerClass(SentencePairFrequencyReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(StringDoubleWritable.class);
        FileInputFormat.addInputPath(job1, unclassifiedInputDir);
        FileOutputFormat.setOutputPath(job1, unclassifiedSentenceBigramFrequencyOutputDir);
        job1.waitForCompletion(true);


        // Job for calculating the english entropy for every row of the unclassified text
        if (hdfs.exists(englishEntropyOutputDir)) {
            hdfs.delete(englishEntropyOutputDir, true);
        }

        Job job2 = Job.getInstance(conf, "calculate entropy for english");
        job2.setJarByClass(DutchEnglishSentenceClassifier.class);
        job2.setMapperClass(EnglishEntropyCalculatorMapper.class);
        job2.setNumReduceTasks(0);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        SequenceFileInputFormat.setInputPaths(job2, englishProbabilityOutputDir, unclassifiedSentenceBigramFrequencyOutputDir);
        FileOutputFormat.setOutputPath(job2, englishEntropyOutputDir);
        job2.waitForCompletion(true);

        // Job for calculating the dutch entropy for every row of the unclassified text
        if (hdfs.exists(dutchEntropyOutputDir)) {
            hdfs.delete(dutchEntropyOutputDir, true);
        }

        Job job3 = Job.getInstance(conf, "calculate entropy for dutch");
        job3.setJarByClass(DutchEnglishSentenceClassifier.class);
        job3.setMapperClass(DutchEntropyCalculatorMapper.class);
        job3.setNumReduceTasks(0);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(DoubleWritable.class);
        SequenceFileInputFormat.setInputPaths(job3, dutchProbabilityOutputDir, unclassifiedSentenceBigramFrequencyOutputDir);
        FileOutputFormat.setOutputPath(job3, dutchEntropyOutputDir);
        job3.waitForCompletion(true);

        // Job for mapping the dutch and english entropy to the row of the unclassified text
        if (hdfs.exists(combinedEntropyOutputDir)) {
            hdfs.delete(combinedEntropyOutputDir, true);
        }

        Job job4 = Job.getInstance(conf, "adds the english and dutch entropy to the bigrams");
        job4.setJarByClass(DutchEnglishSentenceClassifier.class);
        job4.setMapperClass(CombineEntropyMapper.class);
        job4.setReducerClass(CombinedEntropyReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(DoubleDoubleWritable.class);
        SequenceFileInputFormat.setInputPaths(job4, dutchEntropyOutputDir, englishEntropyOutputDir);
        FileOutputFormat.setOutputPath(job4, combinedEntropyOutputDir);
        job4.waitForCompletion(true);


        //Job for classifying the language of every row of the input text
        if (hdfs.exists(classifiedOutputDir)) {
            hdfs.delete(classifiedOutputDir, true);
        }

        Job job5 = Job.getInstance(conf, "Classify every sentence for a unclassified Text");
        job5.setJarByClass(DutchEnglishSentenceClassifier.class);
        job5.setMapperClass(LanguageClassifierMapper.class);
        job5.setCombinerClass(LanguageClassifierReducer.class);
        job5.setReducerClass(LanguageClassifierReducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(IntWritable.class);
        SequenceFileInputFormat.setInputPaths(job5, combinedEntropyOutputDir);
        FileOutputFormat.setOutputPath(job5, classifiedOutputDir);
        System.exit(job5.waitForCompletion(true) ? 0 : 1);
    }

    private static void calculateProbability(
            Path inputDir,
            Path pairFrequencyOutputDir,
            Path letterTotalOutputDir,
            Path probabilityOutputDir,
            Configuration conf,
            FileSystem hdfs
    ) throws Exception {

        if (hdfs.exists(pairFrequencyOutputDir)) {
            hdfs.delete(pairFrequencyOutputDir, true);
        }

        Job job = Job.getInstance(conf, "dutch pair frequency");
        job.setJarByClass(DutchEnglishSentenceClassifier.class);
        job.setMapperClass(PairFrequencyMapper.class);
        job.setCombinerClass(PairFrequencyReducer.class);
        job.setReducerClass(PairFrequencyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, inputDir);
        FileOutputFormat.setOutputPath(job, pairFrequencyOutputDir);
        job.waitForCompletion(true);

        if (hdfs.exists(letterTotalOutputDir)) {
            hdfs.delete(letterTotalOutputDir, true);
        }

        Job job2 = Job.getInstance(conf, "dutch count totals for letter");
        job2.setJarByClass(DutchEnglishSentenceClassifier.class);
        job2.setMapperClass(TotalFrequencyMapper.class);
        job2.setCombinerClass(TotalFrequencyReducer.class);
        job2.setReducerClass(TotalFrequencyReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, pairFrequencyOutputDir);
        FileOutputFormat.setOutputPath(job2, letterTotalOutputDir);
        job2.waitForCompletion(true);

        if (hdfs.exists(probabilityOutputDir)) {
            hdfs.delete(probabilityOutputDir, true);
        }

        // JOB TO CALCULATE THE PROBABILITY BIGRAMS PER TOTAL FIRST LETTER COUNT
        Job job3 = new Job(conf, "dutch calculate probability");
        job3.setJarByClass(DutchEnglishSentenceClassifier.class);
        job3.setMapperClass(PairProbabilityMapper.class);
        job3.setReducerClass(PairProbabilityReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(DoubleDoubleWritable.class);
        job3.setInputFormatClass(TextInputFormat.class);

        SequenceFileInputFormat.setInputPaths(job3, pairFrequencyOutputDir, letterTotalOutputDir);
        FileOutputFormat.setOutputPath(job3, probabilityOutputDir);
        job3.waitForCompletion(true);

        PairProbabilityMapper.clearMaps();


    }
}