package com.hw.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class MyRecordWriter extends RecordWriter<Text,NullWritable> {
    private  String atguiguPath = "F:\\output\\logs\\atguigu.txt";
    private  String otherPath = "F:\\output\\logs\\other.txt";
    private FSDataOutputStream atguigufos;
    private FSDataOutputStream otherfos;

    public MyRecordWriter(TaskAttemptContext job) throws IOException {
        FileSystem fs = FileSystem.get(job.getConfiguration());
        atguigufos = fs.create(new Path(atguiguPath));
        otherfos = fs.create(new Path(otherPath));
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String dataLine = key.toString();
        if (dataLine.contains("atguigu")){
            atguigufos.writeBytes(dataLine+"\n");
        }else {
            otherfos.writeBytes(dataLine+"\n");
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(atguigufos);
        IOUtils.closeStream(otherfos);
    }
}
