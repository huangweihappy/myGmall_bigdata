package com.hw.mapreduce.mapjoin;

import com.hw.mapreduce.outputformat.OutReducer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import javax.xml.transform.OutputKeys;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class MapJoinMapper extends Mapper<LongWritable,Text,Text,NullWritable> {

    private HashMap<String,String> pdMap = new HashMap<>();
    private Text outk = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //在上下文对象context中拿到我们在MapJoinDriver中放在Job对象中的缓存文件路径
        URI[] cacheFiles = context.getCacheFiles();
        URI cacheFile = cacheFiles[0];
        //从文件系统中去获取输入流对象
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(new Path(cacheFile));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fis));
        String line;
        //从文件流中按行读取文件数据并封装到pdMap中
       while((line = bufferedReader.readLine()) != null){
           String[] datas = line.split("\t");

           pdMap.put(datas[0],datas[1]);

       }


//      关闭流对象
        IOUtils.closeStream(fis);

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

//        封装输出的信息
        String[] datas = value.toString().split("\t");
       //1001	01	1
        String data = datas[0]+"\t"+pdMap.get(datas[1])+"\t"+datas[2];

        outk.set(data);

        context.write(outk,NullWritable.get());


    }
}
