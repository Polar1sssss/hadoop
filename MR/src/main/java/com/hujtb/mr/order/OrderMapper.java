package com.hujtb.mr.order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author hujtb
 * @create on 2018-12-20-15:54
 */
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable>{

    OrderBean k = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //000000001 pdt_01 222.22，将订单id和价格封装到OrderBean
        String line = value.toString();
        String[] fields = line.split(" ");
        k.setOrder_id(Integer.parseInt(fields[0]));
        k.setPrice(Double.parseDouble(fields[2]));
        context.write(k, NullWritable.get());
    }
}
