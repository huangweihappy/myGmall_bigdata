package com.hw.hive.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * 做数据清洗的Mapper类
 * 数据
 * 0            1           2   3               4   5       6       7   8       9                 10                11
 * 7D0Mf4Kn4Xk	periurban	583	Music	        201	6508	4.19	687	312 	e2k0h6tPvGc	    yuO6yjlvXe8 	    rjnbgpPJUks
 * SDNkMu8ZT68	w00dy911	630	People & Blogs	186	10181	3.49	494	257	    rjnbgpPJUks
 *
 */
public class GuliVideoETLMapper extends Mapper<LongWritable, Text,Text, NullWritable> {


    private Text outks = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String result = value.toString();


        if(result == null){
            return ;
        }
//        用工具类对数据进行清洗，格式化
        String outk = ETLUtil.GuliVideoETL(result);

        outks.set(outk);
        context.write(outks,NullWritable.get());

    }
}
