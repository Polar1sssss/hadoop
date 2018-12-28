package com.hujtb.mr.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author hujtb
 * @create on 2018-12-21-11:23
 */
public class FRecordWriter extends RecordWriter<Text, NullWritable> {

    FileSystem fs;
    FSDataOutputStream fos1;
    FSDataOutputStream fos2;

    public FRecordWriter(TaskAttemptContext job) {
        try {
            //��ȡ�ļ�ϵͳ
            fs = FileSystem.get(job.getConfiguration());

            //������һ�������
            fos1 = fs.create(new Path("e:/atguigu.log"));

            //�����ڶ��������
            fos2 = fs.create(new Path("e:/other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {

        //�ж�key���Ƿ���atguigu����������д��atguigu.log
        if(key.toString().contains("atguigu")){
            fos1.write(key.toString().getBytes());
        }else{
            fos2.write(key.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(fos1);
        IOUtils.closeStream(fos2);
    }
}
