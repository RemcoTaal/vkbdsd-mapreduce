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
        Path classifiedOutputDir = new Path("output/outputClassified");

        // calculating the probability for dutch text
        calculateProbability(dutchInputDir, dutchPairFrequencyOutputDir, dutchLetterTotalOutputDir, dutchProbabilityOutputDir, conf, hdfs);

        // calculating the probability for english text
        calculateProbability(englishInputDir, englishPairFrequencyOutputDir, englishLetterTotalOutputDir, englishProbabilityOutputDir, conf, hdfs);


        if (hdfs.exists(classifiedOutputDir)) {
            hdfs.delete(classifiedOutputDir, true);
        }

        // Job for classifying the language of every sentence in a input text
        // TODO mapper en reducer van deze job zijn slechts een copy paste, ik heb nog geen logica toegevoegd
        Job job = Job.getInstance(conf, "Classify every sentence for a unclassified Text");
        job.setJarByClass(MaximumEntropyCalculator.class);
        job.setMapperClass(LanguageClassifierMapper.class);
        job.setCombinerClass(LanguageClassifierReducer.class);
        job.setReducerClass(LanguageClassifierReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        SequenceFileInputFormat.setInputPaths(job, unclassifiedInputDir, dutchProbabilityOutputDir, englishProbabilityOutputDir);
        FileOutputFormat.setOutputPath(job, classifiedOutputDir);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
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
        job.setJarByClass(MaximumEntropyCalculator.class);
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
        job2.setJarByClass(MaximumEntropyCalculator.class);
        job2.setMapperClass(TotalFrequencyMapper.class);
        job2.setCombinerClass(TotalFrequencyReducer.class);
        job2.setReducerClass(TotalFrequencyReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, pairFrequencyOutputDir);
        FileOutputFormat.setOutputPath(job2, pairFrequencyOutputDir);
        job2.waitForCompletion(true);

        if (hdfs.exists(probabilityOutputDir)) {
            hdfs.delete(probabilityOutputDir, true);
        }

        // JOB TO CALCULATE THE PROBABILITY BIGRAMS PER TOTAL FIRST LETTER COUNT
        Job job3 = new Job(conf, "dutch calculate probability");
        job3.setJarByClass(MaximumEntropyCalculator.class);
        job3.setMapperClass(PairProbabilityMapper.class);
        job3.setReducerClass(PairProbabilityReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(CompositeWritable.class);
        job3.setInputFormatClass(TextInputFormat.class);

        SequenceFileInputFormat.setInputPaths(job3, pairFrequencyOutputDir, letterTotalOutputDir);
        FileOutputFormat.setOutputPath(job3, probabilityOutputDir);
        job3.waitForCompletion(true);



    }
}
