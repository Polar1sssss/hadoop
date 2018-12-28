package com.hujtb.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author hujtb
 * @create on 2018-12-13-16:41
 */
public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean>{

    long sum_upFlow = 0;
    long sum_downFlow = 0;
    FlowBean v = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        for(FlowBean flowBean : values){
            sum_upFlow += flowBean.getUpFlow();
            sum_downFlow += flowBean.getDownFlow();
        }
        v.set(sum_upFlow, sum_downFlow);
        context.write(key, v);
    }
}
