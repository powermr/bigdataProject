import java.util.Properties

import org.apache.calcite.avatica.ColumnMetaData.StructType
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode, SparkSession, types}
import org.apache.spark.{SparkConf, SparkContext}

object SparkDemo extends Loggable {

  case class Person(id: Int, name: String, age: Int)

  def readMysql(): Unit = {

    //    val hdfsFileNam: String = args(0)
    //    val tableName: String = args(1)
    val tableName: String = "person"
    //    val querySQL: String = args(2)
    val querySQL: String = "select * from person where id>2"

    val conf = new SparkConf()
      .setAppName("Spark sql Test")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext._
    import sqlContext.implicits._
    //    val people = sc.textFile("hdfs://node-1.hdp:8020/griffin/data/batch/demo_src/dt=20180912/hour=09/demo_src")
    val people = sc.textFile("D:\\bigdataProject\\src\\main\\scala\\test.txt")
      //    val people = sc.textFile(hdfsFileNam)
      .map(_.split("\\|"))
      .map(p => Person(p(0).trim.toInt, p(1), p(2).trim.toInt))
      .toDF()
    //    val people = sc.textFile("/user/root/test.txt").map(_.split(",")).map( p => Person(p(0),p(1).trim.toInt,p(2))).toDF()
    //    people.registerTempTable("demo_src")
    people.registerTempTable(tableName)
    val teenagers = sql(querySQL)

    println("==========spark sql demo=======")
    teenagers.map(t => "id:" + t(0) + " name:" + t(1) + " age:" + t(2))
    //      .collect()
    //      .foreach(println)
    val url = "jdbc:mysql://node-1:3306/test?user=root&password=root"
    val connectionProperties = new Properties()
    connectionProperties.setProperty("user", "root"); // 设置用户名
    connectionProperties.setProperty("password", "root");
    teenagers.write.mode(SaveMode.Append).jdbc(url, "person", connectionProperties)
    sc.stop();
  }

  def readCSV_2(): Unit = {
    val sparkSession = SparkSession.builder().master("local").getOrCreate()
    val sc = sparkSession.sparkContext
    val rdd = sc.textFile(
      "D:\\bigdataProject\\src\\main\\scala\\test.txt"
    )
    val rowRDD = rdd.map(_.split("\\|")).map(x => Person(x(0).toInt, x(1), x(2).toInt))
    import sparkSession.implicits._
    val df = rowRDD.toDF()
    import sparkSession.implicits._
    val ds = df.as[Person]
    df.show()
    ds.show()
    sparkSession.stop()
  }

  def readCsv_3(): Unit = {
    val sparkSession = SparkSession.builder().master("yarn").getOrCreate()
    val sc = sparkSession.sparkContext
    val rdd = sc.textFile(
      "/tpm/person_1000.csv"
    )
    val schemaField = "id,name,age"
    val schemaString = schemaField.split(",")
    val schema = types.StructType(
      List(
        StructField(schemaString(0), IntegerType, nullable = false),
        StructField(schemaString(1), StringType, nullable = true),
        StructField(schemaString(2), IntegerType)
      )
    )
    val rowRDD = rdd.map(_.split(",")).map(x => Row(x(0).toInt, x(1), x(2).toInt))
    val df = sparkSession.createDataFrame(rowRDD, schema)
    import sparkSession.implicits._
    val ds = sparkSession.createDataset(rdd)
    df.show()
    ds.show()
    sparkSession.stop()
  }


  def main(args: Array[String]): Unit = {
    //    info(args.toString)
    //    if (args.length < 3) {
    //      error("Usage: class <hdfsFileName> <tableName> <querySQL>")
    //      sys.exit(-1)
    //    }
    //    readMysql
//    readCSV_2
        readCsv_3()
  }
}
