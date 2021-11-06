package com.hw.mapreduce.phone;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PhoneMapper extends Mapper<LongWritable,Text,Text,FlowBean>{

    private Text phoneNumber = new Text();
    private  FlowBean flowBean = new FlowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] datas = line.split("\t");

        phoneNumber.set(datas[1]);
        flowBean.setUpFlow(Integer.parseInt(datas[datas.length-3]));
        flowBean.setDownFlow(Integer.parseInt(datas[datas.length-2]));
        flowBean.setTotalFlow();

        context.write(phoneNumber,flowBean);



    }
}
