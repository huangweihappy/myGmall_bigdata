package com.hw.mapreduce.phone;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PhonePartitioner extends Partitioner<Text,FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        int partition ;
        if (text.toString().startsWith("136")){
            partition = 0 ;
        }else if (text.toString().startsWith("137")){
            partition = 1 ;
        }else if (text.toString().startsWith("138")){
            partition = 2 ;
        }else if (text.toString().startsWith("139")){
            partition = 3 ;
        }else {
            partition = 4 ;
        }


        return partition;
    }
}
