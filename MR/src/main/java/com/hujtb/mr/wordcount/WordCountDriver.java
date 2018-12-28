package com.hujtb.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author hujtb
 * @create on 2018-12-12-15:47
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        //����map�����ѹ��
        conf.setBoolean("mapreduce.map.output.compress", true);
        //����map�����ѹ����ʽ
        conf.setClass("mapreduce.map.output.compress.codec", BZip2Codec.class, CompressionCodec.class);

        //1.��ȡjob����
        Job job = Job.getInstance(conf);

        //2.����jar�洢λ��
        job.setJarByClass(WordCountDriver.class);

        //3.����Map��Reduce��
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //4.����Mapper�׶������k,v����
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //5.���������������(Reducer)��k,v����
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //ֱ��дWordCountReducer.classҲ��
        //job.setCombinerClass(WordCountCombiner.class);

        //����Reduce�����ѹ��
        FileOutputFormat.setCompressOutput(job, true);
        //����Reduce�����ѹ����ʽ
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);

        //6.��������·�������·��
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7.�ύjob
        //job.submit();
        job.waitForCompletion(true);
    }
}
