package com.hujtb.mr.index;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * ��job����
 * ͳ�������ļ�a.txt,b.txt,c.txt�и������ʳ��ִ�����Ҫ�����ļ����ֿ�
 * ������������atguigu a.txt-->3 b.txt-->2 c.txt-->2
 * @author hujtb
 * @create on 2018-12-27-11:11
 */
public class OneIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    String path;
    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        path = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //atguigu pingping
        String line = value.toString();
        String[] fields = line.split(" ");
        for(String word : fields){
            String info = word + "--" + path;
            k.set(info);
            context.write(k, v);
        }
    }
}
