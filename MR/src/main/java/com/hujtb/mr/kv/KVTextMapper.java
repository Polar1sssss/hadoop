package com.hujtb.mr.kv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author hujtb
 * @create on 2018-12-18-11:245
 */
public class KVTextMapper extends Mapper<Text, Text, Text, IntWritable>{

    //输出value的类型
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key, v);
    }
}
