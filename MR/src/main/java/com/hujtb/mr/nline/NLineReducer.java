package com.hujtb.mr.nline;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author hujtb
 * @create on 2018-12-18-15:10
 */
public class NLineReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        IntWritable v = new IntWritable();
        for(IntWritable value : values){
            sum += value.get();
        }
        v.set(sum);
        context.write(key, v);
    }
}
