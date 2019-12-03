package cn.itcast.up.statistics

import cn.itcast.up.base.BaseModel
import org.apache.spark.sql.{Column, DataFrame, Dataset}

object AgeModel extends BaseModel{
  def main(args: Array[String]): Unit = {
    execute()
  }

  /**
    * 获取标签id(即模型id,该方法应该在编写不同模型时进行实现)
    * @return
    */
  override def getTagID(): Int = 14

  /**
    * 开始计算
    * inType=HBase##zkHosts=192.168.10.20##zkPort=2181##
    * hbaseTable=tbl_users##family=detail##selectFields=id,birthday
    * @param fiveDF MySQL中的5级规则 id,rule
    * @param hbaseDF 根据selectFields查询出来的HBase中的数据
    * @return userid,tagIds
    */
  override def compute(fiveDF: DataFrame, hbaseDF: DataFrame): DataFrame = {
    fiveDF.show()
    fiveDF.printSchema()
    /*
+---+-----------------+
| id|             rule|
+---+-----------------+
| 15|19500101-19591231|
| 16|19600101-19691231|
| 17|19700101-19791231|
| 18|19800101-19891231|
| 19|19900101-19991231|
| 20|20000101-20091231|
| 21|20100101-20191231|
| 22|20200101-20291231|
+---+-----------------+
root
 |-- id: long (nullable = false)
 |-- rule: string (nullable = true)
     */
    import spark.implicits._

    val ds: Dataset[(Long, String)] = fiveDF.as[(Long,String)]
    val ruleDF: DataFrame = ds.map(t => {
      val arr: Array[String] = t._2.split("-")
      (arr(0), arr(1), t._1)
    }).toDF("start", "end", "tag")
    ruleDF.show()
    /*
+--------+--------+---+
|   start|     end|tag|
+--------+--------+---+
|19500101|19591231| 15|
|19600101|19691231| 16|
|19700101|19791231| 17|
|19800101|19891231| 18|
|19900101|19991231| 19|
|20000101|20091231| 20|
|20100101|20191231| 21|
|20200101|20291231| 22|
+--------+--------+---+
     */

    hbaseDF.show(10)
    hbaseDF.printSchema()

    /*
+---+----------+
| id|  birthday|
+---+----------+
|  1|1992-05-31|
| 10|1980-10-13|
|100|1993-10-28|
|101|1996-08-18|
|102|1996-07-28|
|103|1987-05-13|
|104|1976-05-08|
|105|1983-10-11|
|106|1984-03-19|
|107|1998-12-29|
+---+----------+
only showing top 10 rows

root
 |-- id: string (nullable = true)
 |-- birthday: string (nullable = true)
     */
    import org.apache.spark.sql.functions._

    val birthdayColumn: Column = regexp_replace(hbaseDF.col("birthday"),"-","")

    val newDF: DataFrame = hbaseDF
      .join(ruleDF,birthdayColumn.between(ruleDF.col("start"),ruleDF.col("end")))
      .select($"id".as("userId"),$"tag".as("tagIds"))
    newDF.show(10)
    /*
 +------+------+
|userId|tagIds|
+------+------+
|     1|    19|
|    10|    18|
|   100|    19|
|   101|    19|
|   102|    19|
|   103|    18|
|   104|    17|
|   105|    18|
|   106|    18|
|   107|    19|
+------+------+
     */
    newDF
  }
}
