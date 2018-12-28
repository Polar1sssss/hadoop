import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author hujtb
 * @create on 2018-12-04-15:49
 */

public class TestFiles {

    @org.junit.Test
    public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {

        //1获取fs对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), conf, "root");

        //2执行上传API
        fs.copyFromLocalFile(new Path("D:\\stream\\output.txt"), new Path("/output.txt"));

        //3关闭资源
        fs.close();
    }

    @org.junit.Test
    public void testCopyToLocalFile() throws URISyntaxException, IOException, InterruptedException {

        //1获取fs对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), conf, "root");

        //2执行复制到本地API
        fs.copyToLocalFile(new Path("/output.txt"), new Path("D:\\output.txt"));

        //3关闭资源
        fs.close();
    }

    @org.junit.Test
    public void testDelete() throws URISyntaxException, IOException, InterruptedException {

        //1获取fs对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), conf, "root");

        //2执行删除API
        fs.delete(new Path("/output.txt"), true);

        //3关闭资源
        fs.close();
    }

    @org.junit.Test
    public void testRename() throws URISyntaxException, IOException, InterruptedException {

        //1获取fs对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), conf, "root");

        //2重命名
        fs.rename(new Path("/sanguo"), new Path("/qunxiong"));

        //3关闭资源
        fs.close();
    }

    @org.junit.Test
    public void testListFiles() throws URISyntaxException, IOException, InterruptedException {

        //1获取fs对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), conf, "root");

        //2查看文件详情
        RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(new Path("/"), true);
        while(remoteIterator.hasNext()){
            LocatedFileStatus locatedFileStatus = remoteIterator.next();
            System.out.println("=========" + locatedFileStatus.getPath().getName() + "===========");
            System.out.println("=========" + locatedFileStatus.getPermission() + "===========");
            System.out.println("=========" + locatedFileStatus.getLen() + "===========");

            BlockLocation[] blockLocations = locatedFileStatus.getBlockLocations();
            for(BlockLocation blockLocation : blockLocations){
                String[] hosts = blockLocation.getHosts();
                for(String host : hosts){
                    System.out.println(host);
                }
            }
            System.out.println("-----------分割线-----------");
        }

        //3关闭资源
        fs.close();
    }

    @org.junit.Test
    public void testListStatus() throws URISyntaxException, IOException, InterruptedException {

        //1获取fs对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop1:9000"), conf, "root");

        //2查看是文件还是文件夹
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for(FileStatus fileStatus :fileStatuses){
            if(fileStatus.isDirectory()){
                System.out.println("d:" + fileStatus.getPath().getName());
            }else{
                System.out.println("f:" + fileStatus.getPath().getName());
            }
        }

        //3关闭资源
        fs.close();
    }
}
