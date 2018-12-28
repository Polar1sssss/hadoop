package com.hujtb.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author hujtb
 * @create on 2018-12-04-14:53
 */
public class HDFSClient {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();

        //设置连接的是哪一个hdfs
        //conf.set("fs.defaultFS", "hdfs://hadoop1:9000");

        //1、获取hdfs客户端对象
        //FileSystem fs = FileSystem.get(conf);
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), conf, "root");

        //2、在hdfs上创建路径
        fs.mkdirs(new Path("/sanguo/lvbu"));

        //3、关闭资源
        fs.close();

        System.out.println("over");
    }
}
