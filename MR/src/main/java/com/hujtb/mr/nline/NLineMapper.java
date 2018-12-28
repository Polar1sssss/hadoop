package com.hujtb.mr.nline;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author hujtb
 * @create on 2018-12-18-15:05
 */
public class NLineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //��ȡһ��
        String line = value.toString();
        //�и�
        String[] words = line.split(" ");
        //ѭ��д��
        for(String word : words){
            k.set(word);
            context.write(k, v);
        }
    }
}
