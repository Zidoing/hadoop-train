package com.zhoulei.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: zhoulei
 * Date: 2019-08-03
 * Time: 17:07
 * To change this template use File | Settings | File Templates.
 * Description:
 */
public class HDFSAPP {

    private static final String HDFS_PATH = "hdfs://node005:8020";

    private FileSystem fileSystem = null;

    private Configuration configuration = null;

    /**
     * 创建hdfs目录
     */
    @Test
    public void mkdir() throws Exception {
        fileSystem.mkdirs(new Path("/hdfsapi/test"));

    }

    /**
     * 创建文件
     *
     * @throws Exception
     */
    @Test
    public void create() throws Exception {
        FSDataOutputStream outputStream = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        outputStream.write("hello hadoop".getBytes());
        outputStream.flush();
        outputStream.close();

    }

    /**
     * 查看hdfs文件内容
     *
     * @throws Exception
     */
    @Test
    public void cat() throws Exception {
        FSDataInputStream inputStream = fileSystem.open(new Path("/hdfsapi/test/a.txt"));
        IOUtils.copyBytes(inputStream, System.out, 1024);
        inputStream.close();

    }

    /**
     * hdfs 重命名
     *
     * @throws Exception
     */
    @Test
    public void rename() throws Exception {
        Path oldPath = new Path("/hdfsapi/test/a.txt");
        Path newPath = new Path("/hdfsapi/test/b.txt");
        fileSystem.rename(oldPath, newPath);
    }

    /**
     * 复制文件到hdfs
     */
    @Test
    public void copyFromLocalFile() throws Exception {
        Path localPath = new Path("/Users/zhoulei/Downloads/Shadowsocks-4.1.2.zip");
        Path hdfsPath = new Path("/hdfsapi/test");
        fileSystem.copyFromLocalFile(localPath, hdfsPath);

    }


    @Test
    public void copyFromLocalFileWithProcess() throws Exception {

        InputStream inputStream = new BufferedInputStream(new FileInputStream(new File("/Users/zhoulei/Downloads/hadoop-2.6.0-cdh5.7.0.tar.gz")));
        FSDataOutputStream outputStream = fileSystem.create(new Path("/hdfsapi/test/hadoop.tar.gz"),
                new Progressable() {
                    @Override
                    public void progress() {
                        System.out.println("."); // 带进度提醒
                    }
                });
        IOUtils.copyBytes(inputStream, outputStream, 2096);

    }

    @Test
    public void coupyToLocalFile() throws Exception {
        Path path = new Path("/hdfsapi/test/b.txt");
        Path localPath = new Path("./");
        fileSystem.copyToLocalFile(path, localPath);
    }

    @Before
    public void setUp() throws Exception {
        System.out.println("HDFSAPP.setUp");

        configuration = new Configuration();
        fileSystem = fileSystem.get(new URI(HDFS_PATH), configuration);

    }

    @Test
    public void listFiles() throws Exception {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/hdfsapi/test"));
        for (FileStatus fileStatus : fileStatuses) {
            String isDir = fileStatus.isDirectory() ? "文件夹" : "文件";
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String s = fileStatus.getPath().toString();
            System.out.println(isDir);
            System.out.println(replication);
            System.out.println(len);
            System.out.println(s);
            System.out.println("---------------------------------");

        }
    }

    @After
    public void tearDown() throws Exception {
        configuration = null;
        fileSystem = null;
        System.out.println("HDFSAPP.tearDown");
    }

}