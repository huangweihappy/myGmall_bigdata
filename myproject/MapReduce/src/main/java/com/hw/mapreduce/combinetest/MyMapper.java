package com.hw.mapreduce.combinetest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper  extends Mapper<LongWritable,Text,Text,IntWritable> {
    private Text outk = new Text();
    private IntWritable outv = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String values = value.toString();
        String[] words = values.split(" ");
        for (String word: words) {
            outk.set(word);
            context.write(outk,outv);
        }

        }
    }

