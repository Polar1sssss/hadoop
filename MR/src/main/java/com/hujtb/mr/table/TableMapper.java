package com.hujtb.mr.table;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author hujtb
 * @create on 2018-12-21-16:41
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    String name;
    Text k = new Text();
    TableBean v = new TableBean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        name = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //1001 1 1
        //1 小米
        String line = value.toString();
        String[] fields = line.split(" ");

        if(name.startsWith("order")){
            v.setOrder_id(fields[0]);
            v.setP_id(fields[1]);
            v.setP_name("");
            v.setAmount(Integer.parseInt(fields[2]));
            v.setFlag("order");
            k.set(fields[1]);
        }else{
            v.setOrder_id("");
            v.setP_id(fields[0]);
            v.setP_name(fields[1]);
            v.setAmount(0);
            v.setFlag("pd");
            k.set(fields[0]);
        }
        context.write(k, v);
    }
}
