package cn.itcast.up.statistics

import cn.itcast.up.base.BaseModel
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, Row}

/**
  * Author itcast
  * Date 2019/10/27 19:07
  * Desc 
  */
object PaymentModel extends BaseModel{
  def main(args: Array[String]): Unit = {
    execute()
  }

  /**
    * 获取标签id(即模型id,该方法应该在编写不同模型时进行实现)
    *
    * @return
    */
  override def getTagID(): Int = 29

  /**
    * 开始计算
    *inType=HBase##zkHosts=192.168.10.20##zkPort=2181##
    * hbaseTable=tbl_orders##family=detail##selectFields=memberId,paymentCode
    * @param fiveDF  MySQL中的5级规则 id,rule
    * @param hbaseDF 根据selectFields查询出来的HBase中的数据
    * @return userid,tagIds
    */
  override def compute(fiveDF: DataFrame, hbaseDF: DataFrame): DataFrame = {
    //fiveDF.show()
    //fiveDF.printSchema()

    //hbaseDF.show(10)
    //hbaseDF.printSchema()
    /*
+---+--------+
| id|    rule|
+---+--------+
| 30|  alipay|
| 31|   wxpay|
| 32|chinapay|
| 33|  kjtpay|
| 34|     cod|
| 35|   other|
+---+--------+

root
 |-- id: long (nullable = false)
 |-- rule: string (nullable = true)

+---------+-----------+
| memberId|paymentCode|
+---------+-----------+
| 13823431|     alipay|
|  4035167|     alipay|
|  4035291|     alipay|
|  4035041|     alipay|
| 13823285|     kjtpay|
|  4034219|     alipay|
|138230939|     alipay|
|  4035083|     alipay|
|138230935|     alipay|
| 13823231|        cod|
+---------+-----------+
only showing top 10 rows

root
 |-- memberId: string (nullable = true)
 |-- paymentCode: string (nullable = true)
     */

    import spark.implicits._
    import scala.collection.JavaConversions._
    import org.apache.spark.sql.functions._

    val ruleMap: Map[String, Long] = fiveDF.collect().map(row=>(row.getString(1),row.getLong(0))).toMap
    println(ruleMap)
    //Map(chinapay -> 32, wxpay -> 31, alipay -> 30, other -> 35, kjtpay -> 33, cod -> 34)

    val tempDS: Dataset[Row] = hbaseDF
      .groupBy("memberId","paymentCode")
      .agg(count("paymentCode").as("counts"))
    tempDS.show(10)
    /*
|memberId|paymentCode|counts|
+--------+-----------+------+
|13823481|     alipay|    96|
| 4035297|     alipay|    80|
|13823317|     kjtpay|    11|
|13822857|     alipay|   100|
| 4034541|     alipay|    96|
| 4034209|        cod|    17|
| 4034863|     alipay|    90|
| 4033371|     alipay|    95|
|13822723|     alipay|   148|
| 4034193|        cod|    16|
+--------+-----------+------+
     */

    //取top1
    val top1DS: Dataset[Row] = tempDS
      .withColumn("rn", row_number.over(Window.partitionBy($"memberId").orderBy($"counts".desc)))
      .where($"rn" === 1)
    //top1DS.show(10)
    /*
+---------+-----------+------+---+
| memberId|paymentCode|counts| rn|
+---------+-----------+------+---+
| 13822725|     alipay|    89|  1|
| 13823083|     alipay|    94|  1|
|138230919|     alipay|    98|  1|
| 13823681|     alipay|    87|  1|
|  4033473|     alipay|   113|  1|
| 13822841|     alipay|    86|  1|
| 13823153|     alipay|   102|  1|
| 13823431|     alipay|    99|  1|
|  4033348|     alipay|   112|  1|
|  4033483|     alipay|    84|  1|
+---------+-----------+------+---+
     */

    val paymentCode2Tag = udf((paymentCode:String)=>{
      var tag: Long = ruleMap.getOrElse(paymentCode,-1)
      if (shapeless.tag == -1){
        tag = ruleMap("other")
      }
      tag
    })

    val newDF: DataFrame = top1DS.select('memberId as "userId",paymentCode2Tag($"paymentCode").as("tagIds"))
    newDF.show(10)
    /*
+---------+------+
|   userId|tagIds|
+---------+------+
| 13822725|    30|
| 13823083|    30|
|138230919|    30|
| 13823681|    30|
|  4033473|    30|
| 13822841|    30|
| 13823153|    30|
| 13823431|    30|
|  4033348|    30|
|  4033483|    30|
+---------+------+
     */

    newDF
  }
}
