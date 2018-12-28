package com.hujtb.mr.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区类
 * @author hujtb
 * @create on 2018-12-20-11:41
 */
public class ProPartitoner extends Partitioner<FlowBean, Text>{
    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
        //获取手机号前三位
        String prePhoneNum = text.toString().substring(0, 3);
        int partition = 4;
        if("135".equals(prePhoneNum)){
            partition = 0;
        }else if("136".equals(prePhoneNum)){
            partition = 1;
        }else if("137".equals(prePhoneNum)){
            partition = 2;
        }else if("138".equals(prePhoneNum)){
            partition = 3;
        }
        return partition;
    }
}
