package xin.taowangtu.hadoop.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HDFSDemo {
    private final static Logger logger = LoggerFactory.getLogger(HDFSDemo.class);
    private FileSystem fs = null;

    public HDFSDemo() {
        String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");

        Configuration conf = new Configuration();
        String coreSiteXml = hadoopConfDir + "\\core-site.xml";
        String hdfsSiteXml = hadoopConfDir + "\\hdfs-site.xml";
        System.out.println(coreSiteXml);
        System.out.println(hdfsSiteXml);
        conf.addResource(new Path(coreSiteXml));
        conf.addResource(new Path(hdfsSiteXml));
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//        conf.set("fs.defaultFS","hdfs://192.168.217.132:9000");
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("", e);
        }
    }

    private boolean createPath() {
        boolean b = false;
        Path path = new Path("/user/mengxb/hadfdemo");
        try {
            b = fs.mkdirs(path);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("", e);
        }
        return b;

    }

    private void upload(String f, String p) {
        Path file = new Path(f);
        Path path = new Path(p);
        try {
            fs.copyFromLocalFile(file, path);
            logger.info(f + " 文件上传成功");
            System.out.println("文件上传成功");
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("", e);
        }
    }

    private boolean deleteHdfsPath(String p) {
        boolean b = false;
        Path path = new Path(p);
        try {
            b = fs.deleteOnExit(path);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("", e);
        }
        return b;
    }

    private boolean renameHdfs(String oldName, String newName) {
        boolean b = false;
        Path oldPath = new Path(oldName);
        Path newPath = new Path(newName);

        try {
            b = fs.rename(oldPath, newPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return b;
    }

    private void recursiveHdfsPath(Path path) {
        FileStatus[] files = null;
        try {
            files = fs.listStatus(path);
            for (FileStatus file : files) {
                if (file.isFile()) {
                    System.out.println(file.getPath().toString());

                } else {
                    recursiveHdfsPath(file.getPath());
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private void copyFileBetweenHdfs(){
        Path inPath=new Path("hdfs://192.168.217.132:9000/user/mengxb/hadfdemo/readFileDemo.py.n");
        Path outPath=new Path("/user/mengxb/readFileDemo.py");

        try {
            FSDataInputStream in = fs.open(inPath);
            FSDataOutputStream out = fs.create(outPath);
            IOUtils.copyBytes(in,out,1024*1024*64,false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        HDFSDemo hd = new HDFSDemo();
        System.out.println(hd);
//        boolean status = hd.createPath(hd.getHadoopFileSystem());
//        hd.upload( "e:\\Hadoop- The Definitive Guide, 4th Edition.pdf", "/user/mengxb/hadfdemo/");
//        hd.deleteHdfsPath("/user/mengxb/hadfdemo/Hadoop- The Definitive Guide, 4th Edition.pdf");
//        if (hd.renameHdfs("/user/mengxb/hadfdemo/readFileDemo.py", "/user/mengxb/hadfdemo/readFileDemo.py.n"))
//            System.out.println("重命名成功！");
//        Path p = new Path("/user/mengxb/");
//        hd.recursiveHdfsPath(p);
hd.copyFileBetweenHdfs();

    }
}
