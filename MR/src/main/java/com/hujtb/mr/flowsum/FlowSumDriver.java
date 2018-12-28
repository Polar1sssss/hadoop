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

        //1.��ȡjob����
        Job job = Job.getInstance(conf);

        //2.����jar�洢λ��
        job.setJarByClass(FlowSumDriver.class);

        //3.����Map��Reduce��
        job.setMapperClass(FlowSumMapper.class);
        job.setReducerClass(FlowSumReducer.class);

        //4.����Mapper�׶������k,v����
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        //5.���������������(Reducer)��k,v����
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //�����Զ��������
        job.setPartitionerClass(ProvincePartitioner.class);
        job.setNumReduceTasks(5);

        //ʹ�úϲ���Ƭ�ķ�ʽ�����������InputFormatClass��Ĭ��ʹ�õ���TextInputFormat.class
        //job.setInputFormatClass(CombineTextInputFormat.class);
        //����洢��Ƭ���ֵ����Ϊ4M
        //CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);

        //6.��������·�������·��
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //7.�ύjob
        //job.submit();
        job.waitForCompletion(true);
    }
}
