package com.hujtb.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *  根据手机号前三位将结果进行分区
 * key是手机号，value是流量
 * @author hujtb
 * @create on 2018-12-19-10:56
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
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
