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
        FileSystem fs = null;
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//        conf.set("fs.defaultFS","hdfs://192.168.217.132:9000");
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

    public static void main(String[] args) {
        HDFSDemo hd = new HDFSDemo();
        FileSystem fs = hd.getHadoopFileSystem();
//        boolean status = hd.createPath(hd.getHadoopFileSystem());
        hd.upload(fs, "D:\\hive\\out\\artifacts\\hive_jar\\hive.jar", "/user/mengxb/hadfdemo/");
    }


}
