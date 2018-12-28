package com.hujtb.mr.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 序列化案例
 * @author hujtb
 * @create on 2018-12-13-15:42
 */
public class FlowSumSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    FlowBean k = new FlowBean();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //读取一行
        String line = value.toString();
        String[] fields = line.split(" ");

        long upFlow = Long.parseLong(fields[2]);
        long downFlow = Long.parseLong(fields[3]);
        long sumFlow = Long.parseLong(fields[4]);

        k.setUpFlow(upFlow);
        k.setDownFlow(downFlow);
        k.setSumFlow(sumFlow);
        v.set(fields[1]);

        context.write(k, v);
    }
}
