package xin.taowangtu.hadoop.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HDFSDemo {
    private final static Logger logger = LoggerFactory.getLogger(HDFSDemo.class);

    private FileSystem getHadoopFileSystem() {
        String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");

        FileSystem fs = null;
        Configuration conf = new Configuration();
        String coreSiteXml = hadoopConfDir + "\\core-site.xml";
        String hdfsSiteXml = hadoopConfDir + "\\hdfs-site.xml";
        System.out.println(coreSiteXml);
        System.out.println(hdfsSiteXml);
        conf.addResource(new Path(coreSiteXml));
        conf.addResource(new Path(hdfsSiteXml));
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//        conf.set("fs.defaultFS","hdfs://192.168.217.132:9000");
        System.out.println(conf.get("fs.defaultFS"));
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("", e);
        }
        return fs;
    }

    private boolean createPath(FileSystem fs) {
        boolean b = false;
        Path path = new Path("/user/mengxb/hadfdemo");
        try {
            b = fs.mkdirs(path);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("", e);
        } finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("", e);
            }
        }
        return b;

    }

    private void upload(FileSystem fs, String f, String p) {
        Path file = new Path(f);
        Path path = new Path(p);
        try {
            fs.copyFromLocalFile(file, path);
            logger.info(f + " 文件上传成功");
            System.out.println("文件上传成功");
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("", e);
        } finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("", e);
            }
        }
    }

    private boolean dropHdfsPath(FileSystem fs, String p) {
        boolean b = false;
        Path path = new Path(p);
        try {
            b = fs.deleteOnExit(path);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("", e);
        } finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("", e);
            }
        }
        return b;
    }


    public static void main(String[] args) {
        HDFSDemo hd = new HDFSDemo();
        FileSystem fs = hd.getHadoopFileSystem();
        System.out.println(fs);
//        boolean status = hd.createPath(hd.getHadoopFileSystem());
        hd.upload(fs, "e:\\Hadoop- The Definitive Guide, 4th Edition.pdf", "/user/mengxb/hadfdemo/");

    }


}
