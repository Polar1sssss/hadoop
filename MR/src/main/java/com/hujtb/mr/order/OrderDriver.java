package com.hujtb.mr.order;

import com.hujtb.mr.wordcount.WordCountDriver;
import com.hujtb.mr.wordcount.WordCountMapper;
import com.hujtb.mr.wordcount.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author hujtb
 * @create on 2018-12-20-15:55
 */
public class OrderDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

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

        //���÷��������࣬�ﵽֻ���ĳ�����ֵ��Ŀ��
        job.setGroupingComparatorClass(OrderGroupingComparator.class);

        //6.��������·�������·��
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7.�ύjob
        //job.submit();
        job.waitForCompletion(true);
    }
}
