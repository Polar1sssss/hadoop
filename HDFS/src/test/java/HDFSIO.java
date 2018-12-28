import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author hujtb
 * @create on 2018-12-04-17:06
 */
public class HDFSIO {

    /**
     * 将本地磁盘文件复制到HDFS
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void putFileToHDFS() throws URISyntaxException, IOException, InterruptedException {

        //获取fs对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), conf, "root");

        //获取输入流和输出流
        FileInputStream fis = new FileInputStream(new File("D:\\stream\\output.txt"));
        FSDataOutputStream fos = fs.create(new Path("/java.txt"));

        //关闭流
        IOUtils.copyBytes(fis, fos, conf);
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }

    /**
     * 从HDFS上下载文件到本地
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void getFilesFromHDFS() throws URISyntaxException, IOException, InterruptedException {

        //获取fs对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), conf, "root");

        //获取输入流和输出流
        FSDataInputStream fis = fs.open(new Path("/java.txt"));
        FileOutputStream fos = new FileOutputStream(new File("D:\\stream\\java1.txt"));
        IOUtils.copyBytes(fis, fos, conf);

        //关闭流
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }

    /**
     * 分块下载--第一块
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void readFilesSeek1() throws URISyntaxException, IOException, InterruptedException {
        //获取fs对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), conf, "root");

        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.7.tar.gz"));
        FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.7.7.tar.gz.part1"));

        // 只读取128M
        byte[] bytes = new byte[1024];
        for(int i = 0; i < 1024 * 128; i++){
            fis.read(bytes);
            fos.write(bytes);
        }

        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }

    /**
     * 分块下载--第二块
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void readFilesSeek2() throws URISyntaxException, IOException, InterruptedException {
        //获取fs对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), conf, "root");

        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.7.tar.gz"));
        //设置下载块的起始位置
        fis.seek(1024 * 1024 * 128);
        FileOutputStream fos = new FileOutputStream(new File("e:/hadoop-2.7.7.tar.gz.part2"));

        // 读取128M之后的块
        IOUtils.copyBytes(fis, fos, conf);
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }
}
