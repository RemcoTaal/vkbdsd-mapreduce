package org.apache.hadoop.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaximumEntropyCalculator {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        Path inputDir = new Path("input");
        Path frequencyOutputDir = new Path("outputFrequency");
        Path totalOutputDir = new Path("outputTotal");
        Path probabilityOutputDir = new Path("outputProbability");

        if(hdfs.exists(frequencyOutputDir)){
            hdfs.delete(frequencyOutputDir, true);
        }

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(MaximumEntropyCalculator.class);
        job.setMapperClass(PairFrequencyMapper.class);
        job.setCombinerClass(PairFrequencyReducer.class);
        job.setReducerClass(PairFrequencyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, inputDir);
        FileOutputFormat.setOutputPath(job, frequencyOutputDir);
        job.waitForCompletion(true);

        if(hdfs.exists(totalOutputDir)){
            hdfs.delete(totalOutputDir, true);
        }

        Job job2 = Job.getInstance(conf, "count totals for letter");
        job2.setJarByClass(MaximumEntropyCalculator.class);
        job2.setMapperClass(TotalFrequencyMapper.class);
        job2.setCombinerClass(TotalFrequencyReducer.class);
        job2.setReducerClass(TotalFrequencyReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, frequencyOutputDir);
        FileOutputFormat.setOutputPath(job2, totalOutputDir);
        job2.waitForCompletion(true);

        // Hier moet een job komen die de 2 inputs (total en frequency) samen voegt en leidt tot een output met lettercombinatie + frequency + probability

        if(hdfs.exists(probabilityOutputDir)){
            hdfs.delete(probabilityOutputDir, true);
        }

        Job probabilityCalculatorJob = new Job(conf, "Calculating the probability of a certain pair");
        probabilityCalculatorJob.setJarByClass(MaximumEntropyCalculator.class);
        MultipleInputs.addInputPath(probabilityCalculatorJob, frequencyOutputDir, TextInputFormat.class, PairFrequencyMapper.class);
        MultipleInputs.addInputPath(probabilityCalculatorJob, totalOutputDir, TextInputFormat.class, TotalFrequencyMapper.class);

        FileOutputFormat.setOutputPath(probabilityCalculatorJob, probabilityOutputDir);


//        probabilityCalculatorJob.setCombinerClass(PairProbabilityReducer.class);
//        probabilityCalculatorJob.setReducerClass(PairProbabilityReducer.class);
//        probabilityCalculatorJob.setNumReduceTasks(1);

        probabilityCalculatorJob.setOutputKeyClass(Text.class);
        probabilityCalculatorJob.setOutputValueClass(IntWritable.class);
        System.exit(probabilityCalculatorJob.waitForCompletion(true) ? 0 : 1);





    }
}

//Job voor nederlandse en engelse paren en frequenties
//Job voor bepalen van de totalen
//Job voor bepalen van aannemelijkheid paren