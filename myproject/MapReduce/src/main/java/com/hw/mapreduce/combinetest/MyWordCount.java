package com.hw.mapreduce.combinetest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyWordCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //声明配置对象
        Configuration configuration = new Configuration();
//        声明Job对象
        Job job = Job.getInstance(configuration);
//        指定当前Job的驱动类
        job.setJarByClass(MyWordCount.class);
//        指定当前job的Mapper和Reducer
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
//        指定当前job的Map段的输出数据的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
//        指定当前job的reduce段的数据输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        指定输入目录和输出目录
        FileInputFormat.setInputPaths(job,new Path("F:\\input\\wcinput"));
        FileOutputFormat.setOutputPath(job , new Path("F:\\output\\wcoutput"));


        job.waitForCompletion(true);




    }
}
