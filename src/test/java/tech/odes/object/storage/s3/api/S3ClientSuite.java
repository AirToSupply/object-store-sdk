package tech.odes.object.storage.s3.api;

import com.amazonaws.services.s3.model.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class S3ClientSuite {

    private static Log LOG = LogFactory.getLog(S3ClientSuite.class);

    private S3Client s3;

    @BeforeEach
    public void setup() {
        s3 = new S3Client.Builder()
            .endpoint("http://127.0.0.1:9977")
            .accessKey("admin")
            .secretKey("admin")
            .bucket("oss")
            .build();
    }

    @AfterEach
    public void down() {
        s3.close();
    }

    @Test
    public void dummy() {
        LOG.info(s3);
    }

    /**
     * 判断文件夹是否存在
     */
    @Test
    public void ed() {
        String path = "/t1/a/";
        LOG.info("[" + path + "] exists result: " + s3.ed(path));
    }

    /**
     * 判断文件是否存在
     */
    @Test
    public void ef() {
        String file = "/t1/a/Bugsnag.x";
        LOG.info("[" + file + "] exists result: " + s3.ef(file));
    }

    /**
     * 创建文件夹
     */
    @Test
    public void mkdir() {
        s3.mkdir("/t1/a");
    }

    @Test
    public void mkdirs() {
        s3.mkdir(Arrays.asList(
            "/c/t1",
            "/c/t2",
            "/c/t3"));
    }

    /**
     * 删除文件
     */
    @Test
    public void rm() {
        s3.rm("/t1/a/Bugsnag.x");
    }

    @Test
    public void rms() {
        s3.rm(Arrays.asList(
            "/t1/a/com.docker.service.config",
            "/t1/a/com.docker.service"));
    }

    /**
     * 删除文件夹
     */
    @Test
    public void rmdir() {
        s3.rmdir("/t1/a/");
    }

    @Test
    public void rmdirs() {
        s3.rmdir(Arrays.asList(
            "/c/t1",
            "/c/t2",
            "/c/t3"));
    }

    /**
     * 查看文件夹下的对象
     */
    @Test
    public void ls() {
        List<S3ObjectSummary> os = s3.ls("/t1/Docker.Engines.pdb");
        os.stream().map(o -> {
            long size = o.getSize(); // b
            String fd = Long.compare(size, 0) == 0 ? "-d" : "-f";
            Date lastModified = o.getLastModified();
            String path = o.getKey();
            return String.format("<%s>\t%s\t%s\t%s", fd, size, lastModified, path);
        }).forEach(o -> System.out.println("|-- " + o));
    }

    /**
     * 拷贝文件
     */
    @Test
    public void cp() {
        s3.cp("/t1/Docker.Engines.pdb", "/t1/a/Docker.Engines.pdb");
    }

    /**
     * 重命名文件
     */
    @Test
    public void mv() {
        s3.mv("/t1/Docker.Core.dll", "/t1/Docker.Core.dll.x");
    }

    /**
     * 上传文件
     */
    @Test
    public void rz() {
        s3.rz("/t1/a/", new File("/dev/zookeeper-3.4.6/README.txt"));
    }

    @Test
    public void rzs() {
        s3.rz("/t1/a/", Arrays.asList(
            new File("/dev/zookeeper-3.4.6/README.txt"),
            new File("C:/Users/Dell/Desktop/core-site.xml")));
    }

    /**
     * 上传文件夹
     */
    @Test
    public void rzdir() {
        s3.rzdir("/t1/a/", new File("/data/test"));
    }

    /**
     * 下载文件
     */
    @Test
    public void sz() {
        s3.sz("/t1/a/Bugsnag.x", new File("/data/test/a"));
    }

    @Test
    public void szs() {
        s3.sz(Arrays.asList(
            "/t1/a/Bugsnag.x",
            "/t1/a/com.docker.service.config",
            "/t1/a/BITSReference5_0.dll"),
            new File("/data/test/a"));
    }

    /**
     * 下载文件夹
     */
    @Test
    public void szdir() {
        s3.szdir("/t1/a", new File("/data/test/a"));
    }

    /**
     * 查看目录下对象数，对象大小
     */
    @Test
    public void du() {
        ObjectListing metadata = s3.du();
        int num = metadata.getObjectSummaries().size();
        AtomicLong size = new AtomicLong(0L);
        metadata.getObjectSummaries().stream().forEach(s -> {
            size.addAndGet(s.getSize());
        });
        LOG.info(String.format("num: %s, size: %d", num, size.get()));
    }
}
