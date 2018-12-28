package com.hujtb.mr.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author hujtb
 * @create on 2018-12-13-18:41
 */
public class FlowSumDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        args = new String[]{"e:/input/propartition", "e:/output1"};
        Configuration conf = new Configuration();

        //1.获取job对象
        Job job = Job.getInstance(conf);

        //2.设置jar存储位置
        job.setJarByClass(FlowSumDriver.class);

        //3.关联Map和Reduce类
        job.setMapperClass(FlowSumMapper.class);
        job.setReducerClass(FlowSumReducer.class);

        //4.设置Mapper阶段输出的k,v类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //5.设置最终数据输出(Reducer)的k,v类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //设置自定义分区类
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);

        //使用合并切片的方式，如果不设置InputFormatClass，默认使用的是TextInputFormat.class
        //job.setInputFormatClass(CombineTextInputFormat.class);
        //虚拟存储切片最大值设置为4M
        //CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        //6.设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7.提交job
        //job.submit();
        job.waitForCompletion(true);
    }
}
