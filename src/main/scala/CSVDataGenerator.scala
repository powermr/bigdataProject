import java.io.FileWriter

import scala.util.Random

object CSVDataGenerator extends Loggable {
  private val pathData = "person.csv"
  private val maxRecord = 100000000
  private val age = 60

  def Create_person(maxRecord: Int, pathData: String): Unit = {
    val rand = new Random()
    val writeData: FileWriter = new FileWriter(pathData, true)

    for (i <- 1 to maxRecord) {
      //年龄
      var dataage = rand.nextInt(age)
      if (dataage < 15) {
        dataage = dataage + 15
      }
      //姓名
      var name = "Test " + BigInt(20, scala.util.Random).toString(32)

      //      println(i+","+name+","+dataage)
      writeData.write(i + "," + name + "," + dataage)
      writeData.write(System.getProperty("line.separator"))
    }
    writeData.flush()
    writeData.close()
    info("产生" + maxRecord + "条 数据，数据文件为" + pathData)
  }

  def Create_vbak(maxRecord: Int, pathData: String): Unit = {
    //    订单编号
//    var vbeln: BigInt
//    var ernam: String = ""
//    var audat:String
  }

  def main(args: Array[String]): Unit = {
    info("生成测试数据:")
    //    Create_person(10000,"person_1w.csv")
    //    Create_person(100000,"person_10w.csv")
    //    Create_person(1000000,"person_100w.csv")
    //    Create_person(10000000,"person_1000w.csv")
    //    Create(1000000000, "E:\\person_10y.csv")

    System.exit(1)

  }
}