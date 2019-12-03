package cn.itcast.up.matchtag

import java.util.Properties

import cn.itcast.up.bean.HBaseMeta
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql._



object JobModel {
  def main(args: Array[String]): Unit = {
    //1.创建SparkSession
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("model")
      .config("spark.hadoop.validateOutputSpecs", "false") //设为false可以忽略文件存在的异常
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._
    //import scala.collection.JavaConversions._
    import org.apache.spark.sql.functions._


    //2.加载MySQL数据
    val url = "jdbc:mysql://bd001:3306/tags_new?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&user=root&password=123456"
    val tableName = "tbl_basic_tag"
    val properties = new Properties()
    val mysqlDF: DataFrame = spark.read.jdbc(url,tableName,properties)

    //3.筛选出4级标签规则
    //val ds: Dataset[Row] = mysqlDF.select("rule").filter("id=7")
    val ds: Dataset[Row] = mysqlDF.select('rule).where('id===7)
    //ds.show(false)
    /*
+-------------------------------------------------------------------------------------------------------------+
|rule                                                                                                         |
+-------------------------------------------------------------------------------------------------------------+
|inType=HBase##zkHosts=192.168.10.20##zkPort=2181##hbaseTable=tbl_users##family=detail##selectFields=id,job|
+-------------------------------------------------------------------------------------------------------------+
     */

    //4.解析规则
    //inType=HBase##zkHosts=192.168.10.20##zkPort=2181##
    //hbaseTable=tbl_users##family=detail##selectFields=id,job
    val fourRuleMap: Map[String, String] = ds.rdd.map(row => {
      val ruleArr: Array[String] = row.getAs[String]("rule")
        .split("##")
      ruleArr.map(kvStr => {
        val kv: Array[String] = kvStr.split("=")
        (kv(0), kv(1))
      }).toMap
    }).collect()(0)
    //println(fourRuleMap)

    //5.将规则分装成HBaseMeta
    val hbaseMeta: HBaseMeta = this.parseMeta(fourRuleMap)

    val fiveDS: Dataset[Row] = mysqlDF
      .select("id", "rule")
      .where("pid=7")
    //fiveDS.show()
    /*
+---+----+
| id|rule|
+---+----+
|  8|   1|
|  9|   2|
| 10|   3|
| 11|   4|
| 12|   5|
| 13|   6|
+---+----+

     */

    //6.筛选出5级标签规则
    //Map[rule, id]
    val fiveRuleMap: Map[String,Long]  = fiveDS
      .map(row => (row.getString(1),row.getLong(0)))
      .collect()
      .toMap
    //println(fiveRuleMap)
    //Map(4 -> 11, 5 -> 12, 6 -> 13, 1 -> 8, 2 -> 9, 3 -> 10)

    //7.加载HBase数据
    val hbaseDF: DataFrame = spark.read
      .format("cn.itcast.up.tools.HBaseSource")
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, hbaseMeta.hbaseTable)
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .option(HBaseMeta.SELECTFIELDS, hbaseMeta.selectFields)
      .option(HBaseMeta.ROWKEY, hbaseMeta.rowKey)
      .load()
    //hbaseDF.show(10)
    //hbaseDF.printSchema()
    /*
+---+---+
| id|job|
+---+---+
|  1|  3|
| 10|  5|
|100|  3|
|101|  1|
|102|  1|
|103|  3|
|104|  6|
|105|  2|
|106|  4|
|107|  1|
+---+---+
only showing top 10 rows

root
 |-- id: string (nullable = true)
 |-- job: string (nullable = true)
     */

    //8.将HBase数据和5级规则进行匹配
    val job2tag = udf((job:String)=>fiveRuleMap(job).toString)

    val newData: DataFrame = hbaseDF.select('id as "userId",job2tag($"job").as("tagIds"))
    //newData.show(10)
    newData.printSchema()

    /*
+------+------+
|userId|tagIds|
+------+------+
|     1|    10|
|    10|    12|
|   100|    10|
|   101|     8|
|   102|     8|
|   103|    10|
|   104|    13|
|   105|     9|
|   106|    11|
|   107|     8|
+------+------+

root
 |-- userId: string (nullable = true)
 |-- tagIds: string (nullable = true)
     */


    //9.将结果进行合并
    val oldData: DataFrame = spark.read
      .format("cn.itcast.up.tools.HBaseSource")
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, "test")
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .option(HBaseMeta.SELECTFIELDS, "userId,tagIds")
      .option(HBaseMeta.ROWKEY, "userId")
      .load()
    //oldData.show(10)
    /*
+------+------+
|userid|tagIds|
+------+------+
|     1|     6|
|    10|     6|
|   100|     6|
|   101|     5|
|   102|     6|
|   103|     5|
|   104|     5|
|   105|     6|
|   106|     5|
|   107|     5|
+------+------+
     */

    val mergeUDF = udf(mergeTag _)

    val tagIdsColumn: Column = mergeUDF(newData("tagIds"),oldData("tagIds"))

    //https://blog.csdn.net/lingbo229/article/details/82464466
    val result: DataFrame = newData
      .join(oldData, newData.col("userId") === oldData("userId"), "left")
      .select(newData.col("userId"), tagIdsColumn.as("tagIds"))
    result.show(10)
    /*
+------+------+
|userId|tagIds|
+------+------+
|   296|  13,5|
|   467|  13,6|
|   675|  10,6|
|   691|   8,5|
|   829|  12,5|
|   125|  12,6|
|   451|   9,6|
|   800|  13,5|
|   853|  11,6|
|   944|  10,6|
+------+------+
     */

    //10.将结果写入到HBase
    result.write
      .format("cn.itcast.up.tools.HBaseSource")
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, "test")
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .option(HBaseMeta.SELECTFIELDS, "userId,tagIds")
      .option(HBaseMeta.ROWKEY, "userId")
      .save()
  }


  /**
    * 解析Map格式规则并返回HBaseMeta对象
    * @param ruleMap
    * @return
    */
  def parseMeta(ruleMap: Map[String, String]): HBaseMeta = {
    HBaseMeta(
      ruleMap.getOrElse(HBaseMeta.INTYPE, null),
      ruleMap.getOrElse(HBaseMeta.ZKHOSTS, null),
      ruleMap.getOrElse(HBaseMeta.ZKPORT, null),
      ruleMap.getOrElse(HBaseMeta.HBASETABLE, null),
      ruleMap.getOrElse(HBaseMeta.FAMILY, null),
      ruleMap.getOrElse(HBaseMeta.SELECTFIELDS, null),
      ruleMap.getOrElse(HBaseMeta.ROWKEY, null)
    )
  }

  /**
    * 合并新旧标签
    * @param newIds
    * @param oldIds
    * @return
    */
  def mergeTag(newIds:String,oldIds:String)={
    if(StringUtils.isBlank(newIds)){
      oldIds
    }else if(StringUtils.isBlank(oldIds)){
      newIds
    }else{
      (newIds.split(",") ++ oldIds.split(","))
        .toSet.mkString(",")
    }
  }
}
