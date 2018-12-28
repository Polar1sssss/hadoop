package com.hujtb.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author hujtb
 * @create on 2018-12-12-15:25
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        //1.�ۼ����
        int sum = 0;
        for(IntWritable value : values){
            sum += value.get();
        }

        //2.д��
        v.set(sum);
        context.write(key, v);
    }
}
