package cn.itcast.up.matchtag

import java.util.Properties

import cn.itcast.up.bean.HBaseMeta
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * 入门案例
  */
object SimpleGenderModel {
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
    //val ds: Dataset[Row] = mysqlDF.select("rule").filter("id=4")
    val ds: Dataset[Row] = mysqlDF.select('rule).where('id===4)
    //ds.show(false)
    /*
+-------------------------------------------------------------------------------------------------------------+
|rule                                                                                                         |
+-------------------------------------------------------------------------------------------------------------+
|inType=HBase##zkHosts=192.168.10.20##zkPort=2181##hbaseTable=tbl_users##family=detail##selectFields=id,gender|
+-------------------------------------------------------------------------------------------------------------+
     */

    //4.解析规则
    //inType=HBase##zkHosts=192.168.10.20##zkPort=2181##
    //hbaseTable=tbl_users##family=detail##selectFields=id,gender
    val fourRuleMap: Map[String, String] = ds.rdd.map(row => {
      val ruleArr: Array[String] = row.getAs[String]("rule")
        .split("##")
      ruleArr.map(kvStr => {
        val kv: Array[String] = kvStr.split("=")
        (kv(0), kv(1))
      }).toMap
    }).collect()(0)
    //println(fourRuleMap)
    //Map(selectFields -> id,gender, inType -> HBase, zkHosts -> 192.168.10.20, zkPort -> 2181, hbaseTable -> tbl_users, family -> detail)

    //5.将规则分装成HBaseMeta
    val hbaseMeta: HBaseMeta = this.parseMeta(fourRuleMap)

    val fiveDS: Dataset[Row] = mysqlDF
      .select("id", "rule")
      .where("pid=4")
    //fiveDS.show()
    /*
+---+----+
| id|rule|
+---+----+
|  5|   1|
|  6|   2|
+---+----+
     */

    //6.筛选出5级标签规则
    //Map[rule, id]
    val fiveRuleMap: Map[String,Long]  = fiveDS
      .map(row => (row.getString(1),row.getLong(0)))
      .collect()
      .toMap
    println(fiveRuleMap)
    //Map(1 -> 5, 2 -> 6)

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
    hbaseDF.show(10)
    hbaseDF.printSchema()
    /*
root
 |-- id: string (nullable = true)
 |-- gender: string (nullable = true)
     */
    /*
+---+------+
| id|gender|
+---+------+
|  1|     2|
| 10|     2|
|100|     2|
|101|     1|
|102|     2|
|103|     1|
|104|     1|
|105|     2|
|106|     1|
|107|     1|
+---+------+
     */


    //8.将HBase数据和5级规则进行匹配
    val gender2Tag = udf((gender:String)=>{
      val tag = fiveRuleMap(gender)
      tag
    })

    val result: DataFrame = hbaseDF
      .select($"id".as("userId"),gender2Tag($"gender").as("tagIds"))
    result.show(10)

    /*
+------+------+
|userId|tagIds|
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

    //9.将结果写入到HBase
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
}
