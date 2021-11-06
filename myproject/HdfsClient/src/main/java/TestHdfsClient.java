import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

/**
 * hdfs的客户端测试程序
 */
public class TestHdfsClient {
    private FileSystem fs;


    /**
     * 下载文件
     * @throws IOException
     */
    @Test
    public void Testdownload() throws IOException {
        fs.copyToLocalFile(false,
                new Path("/mlTest/hello.txt"),
                new Path("D:/temp1"),true);
    }
    @Before
    public void  init() throws  Exception{
        //获取连接hdfs集群的uri地址
        URI uri = URI.create("hdfs://hadoop102:9820");
//        获取配置对象
        Configuration configuration = new Configuration();
//        获取用户
        String user = "hw";
//        获取文件系统连接对象
        fs = FileSystem.get(uri, configuration, user);


    }
    /*
     @param uri of the filesystem
   * @param conf the configuration to use
   * @param user to perform the get as
     */
    @After
    public void close() throws Exception{


//        关闭连接
        fs.close();


    }
}
