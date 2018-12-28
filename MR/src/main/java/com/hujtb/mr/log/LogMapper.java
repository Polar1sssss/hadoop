package com.hujtb.mr.log;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * ������ϴ�� ȥ�����ֶ���С�ڵ���11����
 * @author hujtb
 * @create on 2018-12-25-14:51
 */
public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        boolean result = parseLog(line, context);
        if(!result){
            return;
        }
        context.write(value, NullWritable.get());
    }

    private boolean parseLog(String line, Context context) {
        String[] fields = line.split(" ");
        if(fields.length > 11){
            //ϵͳ������
            context.getCounter("map", "true").increment(1);
            return true;
        }else{
            context.getCounter("map", "false").increment(1);
            return false;
        }
    }
}
