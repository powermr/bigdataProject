package xin.taowangtu.files.tools;

import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class MetadataManager {
    private String path = null;

    public MetadataManager() {
    }

    public MetadataManager(String path) {
        this.path = path;
    }


    public Map<String, List<String>> fileMD5 = new HashMap();

    public Map<String, List<String>> getFileMD5(String path) throws NoSuchAlgorithmException, IOException {
        File file = new File(path);
        if (file.exists()) {
            File[] files = file.listFiles();
            if (null == files || files.length == 0) {
                System.out.println("文件夹是空的。。。");
            } else {
                for (File f : files) {
                    if (f.isDirectory()) {
                        System.out.println("文件夹：" + f.getAbsolutePath());
                        getFileMD5(f.getAbsolutePath());

                    } else {
                        String fileAbsolutePath = f.getAbsolutePath();
                        List<String> fileValue = new ArrayList<>();
//                        System.out.println("文件：" + fileAbsolutePath);
//                        String md5 = DigestUtiltUtils.md5Hex(fileAbsolutePath);
                        byte[] buffer = new byte[1024];
                        MessageDigest diges = MessageDigest.getInstance("MD5");
                        FileInputStream fileInputStream = new FileInputStream(new File(fileAbsolutePath));
                        int length = -1;
                        while ((length = fileInputStream.read(buffer, 0, 1024)) != -1) {
                            diges.update(buffer, 0, length);
                        }
                        if (fileInputStream!=null){

                        fileInputStream.close();
                        }
                        BigInteger bigInt = new BigInteger(1, diges.digest());
                        String md5 = bigInt.toString(16);


                        List<String> tmpValue = fileMD5.get(md5);
                        if (tmpValue == null) {
                            fileValue.add(fileAbsolutePath);
                            fileMD5.put(md5, fileValue);
                        } else {
                            tmpValue.add(fileAbsolutePath);
                            fileMD5.put(md5, tmpValue);
                        }
                    }
                }
            }
        } else {
            System.out.println("文件不存在。。。。");
        }
        return fileMD5;
    }

    public void deleteFile(String fileName) throws IOException {
        File file = new File(fileName);
        System.out.println(fileName);
        FileOutputStream fos = null;
        if (!file.exists()) {
            file.createNewFile();
            // 构造写入文件内容
            fos = new FileOutputStream(file);
            fos.write("Hello Wolrd".getBytes());
        }
        InputStream inputStream = new FileInputStream(file);
        // 关闭流
        if (inputStream != null) {
            inputStream.close();
        }
        file.delete();
    }


    public static void main(String[] args){
        String p=args[0];
        MetadataManager metadataManager = new MetadataManager();
        Map<String, List<String>> fm5 = null;
        try {
            fm5 = metadataManager.getFileMD5(p);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (Map.Entry<String, List<String>> m : fm5.entrySet()) {
            List<String> valueList = m.getValue();
            if (valueList.size() > 1) {
                System.out.println("存在重复文件：");
                int count = 0;
                Map<Integer, String> repMap = new HashMap<>();
                for (String s : valueList) {
                    count += 1;
                    System.out.println("[" + count + "]" + m.getKey() + ": " + s);
                    repMap.put(count, s);
                }
                System.out.println("请输入要删除的文件序号：");
                Scanner sc = new Scanner(System.in);

                int flag = sc.nextInt();
                System.out.println(flag);
                System.out.println(repMap.get(flag));
                try {
                    metadataManager.deleteFile(repMap.get(flag));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }
}
