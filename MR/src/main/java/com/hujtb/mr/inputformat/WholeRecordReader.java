package com.hujtb.mr.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author hujtb
 * @create on 2018-12-18-15:50
 */
public class WholeRecordReader extends RecordReader<Text, BytesWritable>{

    FileSplit fileSplit;
    Configuration conf;
    Text k = new Text();
    BytesWritable v = new BytesWritable();
    //�´���WholeRecordReader�����־�Ż�Ϊtrue
    boolean isProgress = true;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //��ʼ��
        this.fileSplit = (FileSplit)inputSplit;
        conf = taskAttemptContext.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        //����ҵ���߼�����
        if(isProgress){
            //��ȡ��Ƭ���ֽ�����
            byte[] buf = new byte[(int) fileSplit.getLength()];

            //1��ȡfs����
            Path path = fileSplit.getPath();
            FileSystem fs = path.getFileSystem(conf);

            //2��ȡ������
            FSDataInputStream fis = fs.open(path);

            //3ֱ�ӿ���
            IOUtils.readFully(fis, buf, 0, buf.length);

            //4��װvalue
            v.set(buf, 0 , buf.length);

            //5��װkey
            k.set(path.toString());

            //6�ر���Դ
            IOUtils.closeStream(fis);

            isProgress = false;

            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
