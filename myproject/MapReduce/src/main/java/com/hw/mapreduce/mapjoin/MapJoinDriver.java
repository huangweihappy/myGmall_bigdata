package com.hw.mapreduce.mapjoin;

import com.hw.mapreduce.combinetest.MyMapper;
import com.hw.mapreduce.combinetest.MyReducer;
import com.hw.mapreduce.combinetest.MyWordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class MapJoinDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //声明配置对象
        Configuration configuration = new Configuration();
//        声明Job对象
        Job job = Job.getInstance(configuration);
//        指定当前Job的驱动类
        job.setJarByClass(MyWordCount.class);
//        指定当前job的Mapper和Reducer
        job.setMapperClass(MapJoinMapper.class);

//        指定当前job的Map段的输出数据的数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
//        指定当前job的reduce段的数据输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        //设置加载缓存文件的路径
        job.addCacheFile(URI.create("file:///F:/input/cachefile/pd.txt"));
//      不使用ReduceTask 设置任务为0
        job.setNumReduceTasks(0);

//        指定输入目录和输出目录
        FileInputFormat.setInputPaths(job,new Path("F:\\input\\mapjoin"));
        FileOutputFormat.setOutputPath(job , new Path("F:\\output\\mapjoin_out2"));


        job.waitForCompletion(true);
    }
}
