package cn.itcast.up.statistics

import cn.itcast.up.base.BaseModel
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Column, DataFrame}

/**
  * Author itcast
  * Date 2019/10/27 17:22
  * Desc 
  */
object CycleModel extends BaseModel{
  def main(args: Array[String]): Unit = {
    execute()
  }

  /**
    * 获取标签id(即模型id,该方法应该在编写不同模型时进行实现)
    * @return
    */
  override def getTagID(): Int = 23

  /**
    * 开始计算
    * inType=HBase##zkHosts=192.168.10.20##zkPort=2181##hbaseTable=tbl_orders##family=detail##selectFields=memberId,finishTime
    * @param fiveDF  MySQL中的5级规则 id,rule
    * @param hbaseDF 根据selectFields查询出来的HBase中的数据
    * @return userid,tagIds
    */
  override def compute(fiveDF: DataFrame, hbaseDF: DataFrame): DataFrame = {
    fiveDF.show()
    fiveDF.printSchema()

    hbaseDF.show(10)
    hbaseDF.printSchema()

    /*
+---+-----+
| id| rule|
+---+-----+
| 24|  0-7|
| 25| 8-14|
| 26|15-30|
| 27|31-60|
| 28|61-90|
+---+-----+

root
 |-- id: long (nullable = false)
 |-- rule: string (nullable = true)

+---------+----------+
| memberId|finishTime|
+---------+----------+
| 13823431|1564415022|
|  4035167|1565687310|
|  4035291|1564681801|
|  4035041|1565799378|
| 13823285|1565062072|
|  4034219|1563601306|
|138230939|1565509622|
|  4035083|1565731851|
|138230935|1565382991|
| 13823231|1565677650|
+---------+----------+
only showing top 10 rows

root
 |-- memberId: string (nullable = true)
 |-- finishTime: string (nullable = true)
     */
    import spark.implicits._
    import org.apache.spark.sql.functions._
    import scala.collection.JavaConversions._

    val ruleDF: DataFrame = fiveDF.as[(Long, String)].map(t => {
      val arr: Array[String] = t._2.split("-")
      (arr(0), arr(1), t._1)
    }).toDF("start", "end", "tag")


    val tempDF: DataFrame = hbaseDF
      .groupBy("memberId")
      .agg(max($"finishTime".cast(LongType)).as("maxFinishTime"))
    tempDF.show(10)
    /*
 +---------+-------------+
| memberId|maxFinishTime|
+---------+-------------+
|  4033473|   1566022264|
| 13822725|   1566056954|
| 13823681|   1566012541|
|138230919|   1566012606|
| 13823083|   1566048648|
| 13823431|   1566034616|
|  4034923|   1566040092|
|  4033575|   1566039509|
| 13822841|   1566016980|
| 13823153|   1566057403|
+---------+-------------+
     */
    val timeDiffColumn: Column = datediff(
      date_sub(current_timestamp(),30),
      from_unixtime(tempDF.col("maxFinishTime"))
    )

    val newDF: DataFrame = tempDF.join(ruleDF).where(timeDiffColumn.between(ruleDF.col("start"), ruleDF.col("end")))
      .select($"memberId".as("userId"), $"tag".as("tagIds"))
    newDF.show()

    newDF
  }
}
