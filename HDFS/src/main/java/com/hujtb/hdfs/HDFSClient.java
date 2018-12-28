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

        //�������ӵ�����һ��hdfs
        //conf.set("fs.defaultFS", "hdfs://hadoop1:9000");

        //1����ȡhdfs�ͻ��˶���
        //FileSystem fs = FileSystem.get(conf);
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), conf, "root");

        //2����hdfs�ϴ���·��
        fs.mkdirs(new Path("/sanguo/lvbu"));

        //3���ر���Դ
        fs.close();

        System.out.println("over");
    }
}
