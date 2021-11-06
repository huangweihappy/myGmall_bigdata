package com.hw.mapreduce.phone;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {
//    上传流量
    private int upFlow;
//    下行流量
    private int downFlow;
//    总流量
    private int totalFlow;


    public int getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(int upFlow) {
        this.upFlow = upFlow;
    }

    public int getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(int downFlow) {
        this.downFlow = downFlow;
    }

    public int getTotalFlow() {
        return totalFlow;
    }

    public void setTotalFlow(int totalFlow) {
        this.totalFlow = totalFlow;
    }

    public void setTotalFlow() {
        this.totalFlow = this.upFlow+this.downFlow;
    }


    @Override
    public String toString() {
        return upFlow +"\t" + downFlow +"\t" + totalFlow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(upFlow);
        out.writeInt(downFlow);
        out.writeInt(totalFlow);

    }

    @Override
    public void readFields(DataInput in) throws IOException {

        upFlow = in.readInt();
        downFlow = in.readInt();
        totalFlow = in.readInt();
    }
}
