package com.hujtb.mr.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author hujtb
 * @create on 2018-12-27-16:37
 */
public class TwoIndexReducer extends Reducer<Text, Text, Text, Text> {
    Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        StringBuffer sb = new StringBuffer();

        for (Text value : values) {
            sb.append(value.toString().replace("\t", "-->") + "\t");

        }

        v.set(sb.toString());

        context.write(key, v);
    }
}
