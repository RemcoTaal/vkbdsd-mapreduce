package org.apache.hadoop.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
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
        Path dutchEntropyOutputDir = new Path("output/outputEntropyDutch");

        Path englishInputDir = new Path("input/inputEnglish");
        Path englishPairFrequencyOutputDir = new Path("output/outputPairFrequencyEnglish");
        Path englishLetterTotalOutputDir = new Path("output/outputLetterTotalEnglish");
        Path englishProbabilityOutputDir = new Path("output/outputProbabilityEnglish");
        Path englishEntropyOutputDir = new Path("output/outputEntropyEnglish");



        Path unclassifiedPairFrequenciesOutputDir = new Path("output/outputPairFrequencyUnclassified");
        Path unclassifiedInputDir = new Path("input/inputUnclassified");
        Path classifiedOutputDir = new Path("output/outputClassified");
        Path combinedEntropyOutputDir = new Path("output/combinedEntropy");

//         calculating the probability for dutch text
//        calculateProbability(dutchInputDir, dutchPairFrequencyOutputDir, dutchLetterTotalOutputDir, dutchProbabilityOutputDir, conf, hdfs);
//
//         calculating the probability for english text
//        calculateProbability(englishInputDir, englishPairFrequencyOutputDir, englishLetterTotalOutputDir, englishProbabilityOutputDir, conf, hdfs);

//        if (hdfs.exists(dutchPairFrequencyOutputDir)) {
//            hdfs.delete(dutchPairFrequencyOutputDir, true);
//        }
//
//        Job job = Job.getInstance(conf, "dutch pair frequency");
//        job.setJarByClass(MaximumEntropyCalculator.class);
//        job.setMapperClass(PairFrequencyMapper.class);
//        job.setCombinerClass(PairFrequencyReducer.class);
//        job.setReducerClass(PairFrequencyReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(job, dutchInputDir);
//        FileOutputFormat.setOutputPath(job, dutchPairFrequencyOutputDir);
//        job.waitForCompletion(true);
//
//        if (hdfs.exists(dutchLetterTotalOutputDir)) {
//            hdfs.delete(dutchLetterTotalOutputDir, true);
//        }
//
//        Job job2 = Job.getInstance(conf, "dutch count totals for letter");
//        job2.setJarByClass(MaximumEntropyCalculator.class);
//        job2.setMapperClass(TotalFrequencyMapper.class);
//        job2.setCombinerClass(TotalFrequencyReducer.class);
//        job2.setReducerClass(TotalFrequencyReducer.class);
//        job2.setOutputKeyClass(Text.class);
//        job2.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(job2, dutchPairFrequencyOutputDir);
//        FileOutputFormat.setOutputPath(job2, dutchLetterTotalOutputDir);
//        job2.waitForCompletion(true);
//
//        if (hdfs.exists(dutchProbabilityOutputDir)) {
//            hdfs.delete(dutchProbabilityOutputDir, true);
//        }
//
//        // JOB TO CALCULATE THE PROBABILITY BIGRAMS PER TOTAL FIRST LETTER COUNT
//        Job job3 = new Job(conf, "dutch calculate probability");
//        job3.setJarByClass(MaximumEntropyCalculator.class);
//        job3.setMapperClass(PairProbabilityMapper.class);
//        job3.setReducerClass(PairProbabilityReducer.class);
//        job3.setOutputKeyClass(Text.class);
//        job3.setOutputValueClass(FrequencyProbabilityWritable.class);
//        job3.setInputFormatClass(TextInputFormat.class);
//
//        SequenceFileInputFormat.setInputPaths(job3, dutchPairFrequencyOutputDir, dutchLetterTotalOutputDir);
//        FileOutputFormat.setOutputPath(job3, dutchProbabilityOutputDir);
//        job3.waitForCompletion(true);
//
//        PairProbabilityMapper.clearMaps();
//
//        if (hdfs.exists(englishPairFrequencyOutputDir)) {
//            hdfs.delete(englishPairFrequencyOutputDir, true);
//        }
//
//        Job job4 = Job.getInstance(conf, "dutch pair frequency");
//        job4.setJarByClass(MaximumEntropyCalculator.class);
//        job4.setMapperClass(PairFrequencyMapper.class);
//        job4.setCombinerClass(PairFrequencyReducer.class);
//        job4.setReducerClass(PairFrequencyReducer.class);
//        job4.setOutputKeyClass(Text.class);
//        job4.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(job4, englishInputDir);
//        FileOutputFormat.setOutputPath(job4, englishPairFrequencyOutputDir);
//        job4.waitForCompletion(true);
//
//        if (hdfs.exists(englishLetterTotalOutputDir)) {
//            hdfs.delete(englishLetterTotalOutputDir, true);
//        }
//
//        Job job5 = Job.getInstance(conf, "dutch count totals for letter");
//        job5.setJarByClass(MaximumEntropyCalculator.class);
//        job5.setMapperClass(TotalFrequencyMapper.class);
//        job5.setCombinerClass(TotalFrequencyReducer.class);
//        job5.setReducerClass(TotalFrequencyReducer.class);
//        job5.setOutputKeyClass(Text.class);
//        job5.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(job5, englishPairFrequencyOutputDir);
//        FileOutputFormat.setOutputPath(job5, englishLetterTotalOutputDir);
//        job5.waitForCompletion(true);
//
//        if (hdfs.exists(englishProbabilityOutputDir)) {
//            hdfs.delete(englishProbabilityOutputDir, true);
//        }
//
//        // JOB TO CALCULATE THE PROBABILITY BIGRAMS PER TOTAL FIRST LETTER COUNT
//        Job job6 = new Job(conf, "dutch calculate probability");
//        job6.setJarByClass(MaximumEntropyCalculator.class);
//        job6.setMapperClass(PairProbabilityMapper.class);
//        job6.setReducerClass(PairProbabilityReducer.class);
//        job6.setOutputKeyClass(Text.class);
//        job6.setOutputValueClass(FrequencyProbabilityWritable.class);
//        job6.setInputFormatClass(TextInputFormat.class);
//
//        SequenceFileInputFormat.setInputPaths(job6, englishPairFrequencyOutputDir, englishLetterTotalOutputDir);
//        FileOutputFormat.setOutputPath(job6, englishProbabilityOutputDir);
//        job6.waitForCompletion(true);
//
//
//
//        // Job for getting the bigram frequencies of the un classified text
//        if (hdfs.exists(unclassifiedPairFrequenciesOutputDir)) {
//            hdfs.delete(unclassifiedPairFrequenciesOutputDir, true);
//        }
//
//        Job job7 = Job.getInstance(conf, "unclassified pair frequency");
//        job7.setJarByClass(MaximumEntropyCalculator.class);
//        job7.setMapperClass(SentencePairFrequencyMapper.class);
//        job7.setReducerClass(SentencePairFrequencyReducer.class);
//        job7.setOutputKeyClass(IntWritable.class);
//        job7.setOutputValueClass(StringDoubleWritable.class);
//        FileInputFormat.addInputPath(job7, unclassifiedInputDir);
//        FileOutputFormat.setOutputPath(job7, unclassifiedPairFrequenciesOutputDir);
//        job7.waitForCompletion(true);
//
//
//        if (hdfs.exists(englishEntropyOutputDir)) {
//            hdfs.delete(englishEntropyOutputDir, true);
//        }
//
//        Job job9 = Job.getInstance(conf, "calculate entropy for english");
//        job9.setJarByClass(MaximumEntropyCalculator.class);
//        job9.setMapperClass(EnglishEntropyCalculatorMapper.class);
//        job9.setNumReduceTasks(0);
//        job9.setOutputKeyClass(Text.class);
//        job9.setOutputValueClass(DoubleWritable.class);
//        SequenceFileInputFormat.setInputPaths(job9, englishProbabilityOutputDir, unclassifiedPairFrequenciesOutputDir);
//        FileOutputFormat.setOutputPath(job9, englishEntropyOutputDir);
//        job9.waitForCompletion(true);
//
//        if (hdfs.exists(dutchEntropyOutputDir)) {
//            hdfs.delete(dutchEntropyOutputDir, true);
//        }
//        Job job10 = Job.getInstance(conf, "calculate entropy for dutch");
//        job10.setJarByClass(MaximumEntropyCalculator.class);
//        job10.setMapperClass(DutchEntropyCalculatorMapper.class);
//        job10.setNumReduceTasks(0);
//        job10.setOutputKeyClass(Text.class);
//        job10.setOutputValueClass(DoubleWritable.class);
//        SequenceFileInputFormat.setInputPaths(job10, dutchProbabilityOutputDir, unclassifiedPairFrequenciesOutputDir);
//        FileOutputFormat.setOutputPath(job10, dutchEntropyOutputDir);
//        job10.waitForCompletion(true);
//
//        if (hdfs.exists(combinedEntropyOutputDir)) {
//            hdfs.delete(combinedEntropyOutputDir, true);
//        }
//        Job job11 = Job.getInstance(conf, "adds the english and dutch entropy to the bigrams");
//        job11.setJarByClass(MaximumEntropyCalculator.class);
//        job11.setMapperClass(CombineEntropyMapper.class);
//        job11.setNumReduceTasks(0);
//        job11.setOutputKeyClass(Text.class);
//        job11.setOutputValueClass(DoubleDoubleWritable.class);
//        SequenceFileInputFormat.setInputPaths(job11, dutchEntropyOutputDir, englishEntropyOutputDir);
//        FileOutputFormat.setOutputPath(job11, combinedEntropyOutputDir);
//        job11.waitForCompletion(true);



        //Job for classifying the language of every sentence in a input text
        if (hdfs.exists(classifiedOutputDir)) {
            hdfs.delete(classifiedOutputDir, true);
        }
        Job job8 = Job.getInstance(conf, "Classify every sentence for a unclassified Text");
        job8.setJarByClass(MaximumEntropyCalculator.class);
        job8.setMapperClass(LanguageClassifierMapper.class);
//        job8.setCombinerClass(LanguageClassifierReducer.class);
        job8.setReducerClass(LanguageClassifierReducer.class);
        job8.setOutputKeyClass(Text.class);
        job8.setOutputValueClass(IntWritable.class);

        SequenceFileInputFormat.setInputPaths(job8, combinedEntropyOutputDir);
        FileOutputFormat.setOutputPath(job8, classifiedOutputDir);

        System.exit(job8.waitForCompletion(true) ? 0 : 1);
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
        FileOutputFormat.setOutputPath(job2, letterTotalOutputDir);
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
        job3.setOutputValueClass(DoubleDoubleWritable.class);
        job3.setInputFormatClass(TextInputFormat.class);

        SequenceFileInputFormat.setInputPaths(job3, pairFrequencyOutputDir, letterTotalOutputDir);
        FileOutputFormat.setOutputPath(job3, probabilityOutputDir);
        job3.waitForCompletion(true);



    }
}
