package com.hujtb.mr.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ���л�����
 * @author hujtb
 * @create on 2018-12-13-15:42
 */
public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    Text k = new Text();
    FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //��ȡһ��
        String line = value.toString();
        //�����зָ������
        String[] fields = line.split(" ");
        //���ֻ�������Ϊ�����key
        k.set(fields[1]);
        long upFlow = Long.parseLong(fields[fields.length - 3]);
        long downFlow = Long.parseLong(fields[fields.length - 2]);
        v.setUpFlow(upFlow);
        v.setDownFlow(downFlow);

        context.write(k, v);
    }
}
