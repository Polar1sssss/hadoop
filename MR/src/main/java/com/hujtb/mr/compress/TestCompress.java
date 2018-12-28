package com.hujtb.mr.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

/**
 * @author hujtb
 * @create on 2018-12-26-10:32
 */
public class TestCompress {
    public static void main(String[] args) throws Exception {
        compress("e:/hello.txt", "org.apache.hadoop.io.compress.BZip2Codec");
        deCompress("e:/hello.tar.gz");
    }

    /**
     * 解压缩
     * @param fileName
     * @throws Exception
     */
    private static void deCompress(String fileName) throws Exception {
        //0.校验是否能够解压缩
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(fileName));
        if(codec == null){
            System.out.println("cannot be unzipped");
            return;
        }
        //1.创建输入流
        FileInputStream fis = new FileInputStream(new File(fileName));
        CompressionInputStream cis = codec.createInputStream(fis);
        //2.创建输出流
        FileOutputStream fos = new FileOutputStream(new File(fileName));
        //3.流的对拷
        IOUtils.copyBytes(cis, fos, 1024*1024, false);
        //4.关闭流
        IOUtils.closeStream(fos);
        IOUtils.closeStream(cis);
        IOUtils.closeStream(fis);
    }

    /**
     * 压缩
     * @param fileName
     * @param method
     * @throws Exception
     */
    private static void compress(String fileName, String method) throws Exception {
        //1.获取输入流
        FileInputStream fis = new FileInputStream(new File(fileName));
        Class codeClass = Class.forName(method);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codeClass, new Configuration());
        //2.获取输出流
        FileOutputStream fos = new FileOutputStream(new File(fileName + codec.getDefaultExtension()));
        //将普通格式输出流转换为压缩输出流
        CompressionOutputStream cos = codec.createOutputStream(fos);
        //3.流的对拷
        IOUtils.copyBytes(fis, cos, 1024*1024, false);
        //4.流的关闭
        IOUtils.closeStream(cos);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }
}
