package com.hujtb.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN 输入数据key
 * VALUEIN 输入数据的value
 * KEYOUT 输出数据key
 * VALUEOUT 输出数据value
 * @author hujtb
 * @create on 2018-12-12-11:45
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println(key);
        //atguigu atguigu
        String line = value.toString();
        String[] words = line.split(" ");

        for(String word : words){
            k.set(word);
            context.write(k, v);
        }
    }

}
