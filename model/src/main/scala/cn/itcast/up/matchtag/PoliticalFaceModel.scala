package cn.itcast.up.matchtag

import java.util.Properties

import cn.itcast.up.bean.HBaseMeta
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * 政治面貌标签开发
  */
object PoliticalFaceModel {

  def main(args: Array[String]): Unit = {
    //1.环境
    //1.创建SparkSQL环境
    val spark: SparkSession = SparkSession.builder()
      .appName("JobModel")
      .master("local[*]")
      .getOrCreate()
    //导入隐式转换
    import org.apache.spark.sql.functions._
    import spark.implicits._
    //导入集合转换包
    //2.加载MySQL
    val url = "jdbc:mysql://bd001:3306/tags_new?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&user=root&password=123456"
    val tableName = "tbl_basic_tag"
    val properties = new Properties
    //标签的ID
    val pid = 352


    val mysqlDF: DataFrame = spark.read.jdbc(url, tableName, properties)

    //3.加载4级数据源信息
    val fourDs: Dataset[Row] = mysqlDF.select('id, 'rule)
      .where("id = "+ pid)
    //将数据源信息提取出来.
    val params: Map[String, String] = fourDs.rdd.map(row => {
      //inType=HBase##zkHosts=192.168.10.20##zkPort=2181##hbaseTable=tbl_users##family=detail##selectFields=id,politicalFace
      val ruleSource: String = row.getAs("rule").toString
      ruleSource.split("##")
        .map(str => {
          val arr: Array[String] = str.split("=")
          (arr(0), arr(1))
        }).toMap
    }).collect()(0)

    println(params)

    //4.加载5级标签规则信息
    val fiveDs: Dataset[Row] = mysqlDF.select('id, 'rule)
      .where("pid = " + pid)
    //将数据转换为Map类型
    val fiveRuleMap: Map[String, String] = fiveDs.map(row => {
      //将row转换成map
      val id = row.getAs("id").toString
      val rule = row.getAs("rule").toString
      (rule, id)
    }).collect().toMap
    println(fiveRuleMap)

    //5.加载HBase数据
//    params
    val hbaseDF: DataFrame = spark.read
      .format("cn.itcast.userprofile.model.tools.HBaseDataSource")
      //将4级规则的map传入.
      .options(params)
      .load()

    hbaseDF.show()
/*
    +---+-------------+
    | id|politicalFace|
    +---+-------------+
    |  1|            1|
    | 10|            2|
    |100|            2|
    |101|            1|
    |102|            3|
*/

    //6.进行标签匹配

    val getTag = udf((politicalFace: String) =>{
      fiveRuleMap.getOrElse(politicalFace, "")
    })

    val resultDF: DataFrame = hbaseDF.select(
      'id.as("userid"),
      getTag('politicalFace).as("tagIds")
    )
    resultDF.show()



    //7.数据合并/落地.
//    解析HBase数据参数
    val meta: HBaseMeta = JobModel.parseMeta(params)
    //加载历史数据
    val oldDF: DataFrame = spark.read
      .format("cn.itcast.userprofile.model.tools.HBaseDataSource")
      .option(HBaseMeta.ZKHOSTS, meta.zkHosts)
      .option(HBaseMeta.ZKPORT, meta.zkPort)
      .option(HBaseMeta.HBASETABLE, "test03")
      .option(HBaseMeta.FAMILY, meta.family)
      .option(HBaseMeta.SELECTFIELDS, "userid,tagIds")
      .load()
    //合并新老数据
    //自定义函数,进行标签的合并
    val mergeTag = udf((newTag: String, oldTag: String) => {
      //对新老数据进行非空判断
      if (StringUtils.isBlank(newTag) && StringUtils.isBlank(oldTag)){
        //如果两个都为空,返回""
        ""
      } else if (StringUtils.isBlank(newTag)) {
        oldTag
      } else if (StringUtils.isBlank(oldTag)) {
        newTag
      } else {
        //如果新老数据都不为空,那么就进行字符串拼接
        //222,444,555,666,222
        val tagStr = newTag + "," + oldTag
        tagStr.split(",").toList
          //对标签进行去重.
          .toSet
          //将集合按照指定的分隔符进行分隔,然后合并为一个字符串
          .mkString(",")
      }

    })
    val fullDF: DataFrame = resultDF.join(oldDF,resultDF.col("userid") === oldDF.col("userid"),"full")
    val saveDF: DataFrame = fullDF.select(
      //userid,tagIds
      when(resultDF.col("userid").isNotNull, resultDF.col("userid"))
        .when(resultDF.col("userid").isNull, oldDF.col("userid"))
        .as("userid"),
      //调用自定义函数,将新数据的tag和老数据的tag传入,进行计算.得到最终的标签
      mergeTag(resultDF.col("tagIds"), oldDF.col("tagIds"))
        .as("tagIds")
    )
    //保存合并结果.
    saveDF.write
      .format("cn.itcast.userprofile.model.tools.HBaseDataSource")
      .option(HBaseMeta.ZKHOSTS, meta.zkHosts)
      .option(HBaseMeta.ZKPORT, meta.zkPort)
      .option(HBaseMeta.HBASETABLE, "test03")
      .option(HBaseMeta.FAMILY, "detail")
      .option(HBaseMeta.SELECTFIELDS, "userid,tagIds")
      .save()

  }
}
