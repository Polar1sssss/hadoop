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
    //新创建WholeRecordReader对象标志才会为true
    boolean isProgress = true;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //初始化
        this.fileSplit = (FileSplit)inputSplit;
        conf = taskAttemptContext.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        //核心业务逻辑处理
        if(isProgress){
            //获取切片的字节数组
            byte[] buf = new byte[(int) fileSplit.getLength()];

            //1获取fs对象
            Path path = fileSplit.getPath();
            FileSystem fs = path.getFileSystem(conf);

            //2获取输入流
            FSDataInputStream fis = fs.open(path);

            //3直接拷贝
            IOUtils.readFully(fis, buf, 0, buf.length);

            //4封装value
            v.set(buf, 0 , buf.length);

            //5封装key
            k.set(path.toString());

            //6关闭资源
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
