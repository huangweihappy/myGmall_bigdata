package com.hw.mapreduce.phone;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PhoneReducer extends Reducer<Text ,FlowBean,Text,FlowBean>{
    private FlowBean flowBean = new FlowBean();
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        int upSumFlow = 0;
        int downSumFlow = 0;
        for (FlowBean flow : values) {
            upSumFlow += flow.getUpFlow();
            downSumFlow += flow.getDownFlow();
        }
        flowBean.setUpFlow(upSumFlow);
        flowBean.setDownFlow(downSumFlow);
        flowBean.setTotalFlow();


        context.write(key, flowBean);


    }
}
