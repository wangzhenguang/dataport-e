import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class HdfsTest {

    FileSystem fs = null;
    Configuration conf = null;

    @Before
    public void before() throws IOException {
        conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop1:9000");
        fs = FileSystem.get(conf);
    }

    @Test
    public void testUpload() throws IOException {
//        fs.copyFromLocalFile(new Path("查询的数据.txt"), new Path("/test/查询的数据.txt"));
//        fs.close();
//        fs.copyFromLocalFile(new Path("flow.log"), new Path("/mr_test/flow.log"));
//        fs.close();
    }


    @Test
    public void testDownload() throws IOException {
        fs.copyToLocalFile(new Path("/test/查询的数据.txt"), new Path("查洵的数据.txt.hdfs"));
        fs.close();
    }

    @Test
    public void testConf() {
        Iterator<Map.Entry<String, String>> iterator = conf.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> next = iterator.next();
            System.out.println(next.getKey() + ": " + next.getValue());
        }
    }

    /**
     * 创建目录
     */
    @Test
    public void makdirTest() throws Exception {
        boolean mkdirs = fs.mkdirs(new Path("/aaa/bbb"));
        System.out.println(mkdirs);
    }

    /**
     * 删除
     */
    @Test
    public void deleteTest() throws Exception{
        boolean delete = fs.delete(new Path("/mr_test"), true);//true， 递归删除
        System.out.println(delete);
    }

    @Test
    public void listTest() throws Exception{
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : listStatus) {
            System.err.println(fileStatus.getPath()+"================="+fileStatus.toString());
        }
        //会递归找到所有的文件
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while(listFiles.hasNext()){
            LocatedFileStatus next = listFiles.next();
            String name = next.getPath().getName();
            Path path = next.getPath();
            System.out.println(name + "---" + path.toString());
        }
    }


}
