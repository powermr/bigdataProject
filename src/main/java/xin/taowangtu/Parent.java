package xin.taowangtu;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLUseStatement;
import com.alibaba.druid.sql.parser.ParserException;
import com.alibaba.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.druid.stat.TableStat;
import com.alibaba.druid.util.JdbcConstants;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * @program: Parent
 * @description: 血缘分析，根据sql脚本内容，输出表名。
 * @author: TaoWangTu
 * @create: 2021年7月8日19:58:27
 **/
public class Parent {
    /**
     * @Description: 获取sql中的表名
     * @Param: [sql]
     * @return: java.util.Map<java.lang.String, java.util.TreeSet < java.lang.String>>
     * @Author: TaoWangTu
     * @Date: 2021/7/8
     */
    public static Map<String, TreeSet<String>> getFromTo(String sql) throws ParserException {
        Map<String, TreeSet<String>> result = new HashMap<String, TreeSet<String>>();
//        解析sql
        List<SQLStatement> stmts = SQLUtils.parseStatements(sql, JdbcConstants.HIVE);
        TreeSet<String> selectSet = new TreeSet<String>();
        TreeSet<String> updateSet = new TreeSet<String>();
        TreeSet<String> insertSet = new TreeSet<String>();
        TreeSet<String> deleteSet = new TreeSet<String>();

        if (stmts == null) {
            return null;
        }

        String database = "DEFAULT";
        for (SQLStatement stmt : stmts) {
            SchemaStatVisitor statVisitor = SQLUtils.createSchemaStatVisitor(stmts, JdbcConstants.HIVE);
            if (stmt instanceof SQLUseStatement) {
                //获取数据库名
                database = ((SQLUseStatement) stmt).getDatabase().getSimpleName();
            }
            stmt.accept(statVisitor);
            Map<TableStat.Name, TableStat> tables = statVisitor.getTables();

            if (tables != null) {
                final String db = database;
                for (Map.Entry<TableStat.Name, TableStat> table : tables.entrySet()) {
                    TableStat.Name tableName = table.getKey();
                    TableStat stat = table.getValue();

                    if (stat.getCreateCount() > 0 || stat.getInsertCount() > 0) { //create
                        String insert = tableName.getName();
                        if (!insert.contains("."))
                            insert = db + "." + insert;
                        insertSet.add(insert);
                    } else if (stat.getSelectCount() > 0) { //select
                        String select = tableName.getName();
                        if (!select.contains("."))
                            select = db + "." + select;
                        selectSet.add(select);
                    } else if (stat.getUpdateCount() > 0) { //update
                        String update = tableName.getName();
                        if (!update.contains("."))
                            update = db + "." + update;
                        updateSet.add(update);
                    } else if (stat.getDeleteCount() > 0) { //delete
                        String delete = tableName.getName();
                        if (!delete.contains("."))
                            delete = db + "." + delete;
                        deleteSet.add(delete);
                    }
                }
            }
        }

        result.put("select", selectSet);
        result.put("insert", insertSet);
        result.put("update", updateSet);
        result.put("delete", deleteSet);

        return result;
    }

    /**
    * @Description: 读取sql文件
    * @Param: [filePath]
    * @return: java.lang.String 返回sql脚本内容
    * @Author: TaoWangTu
    * @Date: 2021/7/8
    */
    public static String  readText(String filePath){
        StringBuffer sql=new StringBuffer();

        try {
            File file = new File(filePath);
            if(file.isFile() && file.exists()) {
                InputStreamReader isr = new InputStreamReader(new FileInputStream(file), "utf-8");
                BufferedReader br = new BufferedReader(isr);
                String lineTxt = null;
                while ((lineTxt = br.readLine()) != null) {
                    sql.append(lineTxt);
                }
                br.close();
            } else {
                System.out.println("文件不存在!");
            }
        } catch (Exception e) {
            System.out.println("文件读取错误!");
        }
        return sql.toString();
    }

    public static void main(String[] args) {
        //可以解析脚本中的数据库和表名包括use语句中的库名。
//        String sql = "use test;select * from " +
//                "(select * from student d where dt='20190202')a " +
//                "left join " +
//                "(select * from supindb.college c where dt='20190202')b " +
//                " on a.uid=b.uid " +
//                "where a.uid > 0;" +
//                "update supindb.college set uid='22333' where name='小明'";

//        String sql = "update supindb.college set uid='22333' where name='小明'";
        //sql = "delete from supindb.college where uid= '22223333'";
        String path="D:\\bigdataProject\\src\\main\\resources\\test.sql";
        String sql=readText(path);
        Map<String, TreeSet<String>> getfrom = getFromTo(sql);

        for (Map.Entry<String, TreeSet<String>> entry : getfrom.entrySet()) {
            System.out.println("================");
            System.out.println("key=" + entry.getKey());
            for (String table : entry.getValue()) {
                System.out.println(table);
            }
        }
    }
}
