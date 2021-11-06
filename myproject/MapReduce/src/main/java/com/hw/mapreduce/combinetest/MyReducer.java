package com.hw.mapreduce.combinetest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 基于wordCount自己编写的 Reducer类
 */
public class MyReducer extends Reducer<Text,IntWritable,Text ,IntWritable> {

    private IntWritable outv = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int total = 0 ;//统计单词出现的个数
        //value里面的值对应一个1的值  (msg,1),(msg,1),(msg,1)... 当key一样时统计value
        for (IntWritable value : values) {
            total += value.get();
        }
        outv.set(total);
        context.write(key,outv);
    }
}
