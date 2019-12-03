package cn.itcast.up.matchtag

import java.util.Properties

import cn.itcast.up.bean.HBaseMeta
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object NationalityModel {

  def main(args: Array[String]): Unit = {
//    1. 先创建标签(web界面创建).
    val id = "419"
    //2. 构建SparkSQL执行环境
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("NationalityModel")
      .getOrCreate()
    //隐式类型转换
    import org.apache.spark.sql.functions._
    import spark.implicits._
    //3. 加载MySQL数据源(获取MySQL中的表数据`tbl_basic_tag`)
    //   1. spark.read.jdbc(url,tableName,properties)
    val url = "jdbc:mysql://bd001:3306/tags_new?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC&user=root&password=123456"
    val tableName = "tbl_basic_tag"
    val properties = new Properties
    val mysqlSource: DataFrame = spark.read.jdbc(url,tableName,properties)
    //4. 获取4级标签规则(数据源相关的信息/配置,比如是从HBase获取数据还是从Hive中获取数据.)
    //   1. mysqlSource.select(‘rule).where(“id = 401”)
    val fourDS: Dataset[Row] = mysqlSource.select('rule).where("id = " + id)
    //   2. 将4级数据转换row=>HBaseMeta/Map
    //inType=HBase##zkHosts=192.168.10.20##zkPort=2181##hbaseTable=tbl_users##family=detail##selectFields=id,nationality
    val fourMap: Map[String, String] = fourDS.map(row => {
      val sourceStr: String = row.getAs("rule").toString
      sourceStr.split("##")
        .map(kv => {
          val arr: Array[String] = kv.split("=")
          (arr(0), arr(1))
        })
    }).collect()(0).toMap
    //将map转换为HBaseMeta
    val meta = HBaseMeta(fourMap)
    //5. 获取5级标签规则(打标签的参考值)
    //   1. mysqlSource.select(‘id,’rule).where(“pid = 401”)
    val fiveDS: Dataset[Row] = mysqlSource.select('id, 'rule).where("pid = " + id)
    //   2. 将5级数据封装成row => TagRule/Map
    //6. 创建HBase的数据源
    //   1. 根据4级的规则信息,比如zk信息,表名字,选择的字段.
    val hbaseSource: DataFrame = spark.read
      .format("cn.itcast.up.model.tools.HBaseDataSource")
      .options(fourMap)
      .load()
//    hbaseSource.show()
//    +---+-----------+
//    | id|nationality|
//    +---+-----------+
//    |  1|          1|
//      | 10|          1|
//      |100|          1|
//      |101|          1|
//      |102|          1|
    //7. 根据5级标签规则进行标签计算
    //   1. when(条件gender===1, 402)
    //   2. UDF函数获取,用当前的值去匹配TagRule,如果能匹配上,就获取当前标签
    //   3. 使用数据源和5级规则数据进行join关联.
    val joinResult: DataFrame = hbaseSource.join(fiveDS, 'nationality === 'rule)
//    joinResult.show()
//    +---+-----------+---+----+
    //| id|nationality| id|rule|
    //+---+-----------+---+----+
    //|  1|          1|420|   1|
    //| 10|          1|420|   1|
    //|100|          1|420|   1|
    //|101|          1|420|   1|
    //|102|          1|420|   1|
    //|103|          1|420|   1|

    //筛选我们需要的字段
    val result: DataFrame = joinResult.select(hbaseSource.col("id").as("userid"), fiveDS.col("id").as("tagIds"))
//    result.show()
//+------+------+
    //|userid|tagIds|
    //+------+------+
    //|     1|   420|
    //|    10|   420|
    //|   100|   420|
    //|   101|   420|
    //|   102|   420|
    //|   103|   420|

    //新老数据合并
    // 1. 获取之前的老数据
    val oldDF: DataFrame = spark.read
      .format("cn.itcast.up.model.tools.HBaseDataSource")
      //去test30这张表查询userid,tagIds
      .option(HBaseMeta.ZKHOSTS, meta.zkHosts)
      .option(HBaseMeta.ZKPORT, meta.zkPort)
      .option(HBaseMeta.FAMILY, meta.family)
      .option(HBaseMeta.HBASETABLE, "test30")
      .option(HBaseMeta.SELECTFIELDS, "userid,tagIds")
      .load()

    //使用自定义函数,进行标签的合并
    //将新老数据的tagIds都传入此方法,进行合并
    val mergeTag = udf((newTag:String,oldTag: String)=>{
      //最终的返回值
      var tagIds = ""
      //判断新老数据是否为空
      if (StringUtils.isBlank(newTag)){
        //新数据为空,返回老数据
        tagIds = oldTag
      }
      if (StringUtils.isBlank(oldTag)){
        //老数据为空,返回新数据
        tagIds = newTag
      }
      if (StringUtils.isNotBlank(newTag) && StringUtils.isNotBlank(oldTag)){
        //新数据不为空,老数据不为空,新老数据开始合并
        //311     322,334,311   =>   311,322,334,311
        val mergeTagStr: String = newTag + "," + oldTag
        //去重.使用","切分,转成Set
        val resultStr: String = mergeTagStr.split(",").toSet.mkString(",")
        tagIds = resultStr
      }
      tagIds
    })



    // 2. 新老数据进行join关联操作.
    val saveDF: DataFrame = result
      //开始关联
      .join(oldDF, result.col("userid") === oldDF.col("userid"), "full")
      //开始进行标签合并
      .select(
      //select最终结果2列就ok了. userid, tagIds
      when(result.col("userid").isNotNull, result.col("userid"))
        .when(result.col("userid").isNull, oldDF.col("userid"))
        .as("userid"),
      mergeTag(result.col("tagIds"), oldDF.col("tagIds")).as("tagIds")
    )
//    saveDF.show
  //+------+-------+
      //|userid| tagIds|
      //+------+-------+
      //|   296|420,411|
      //|   467|420,411|
      //|   675|420,408|
      //|   691|420,406|
      //|   829|420,410|

    //8. 将结果标签存入HBase
    //   1. 使用自定义数据源进行保存

    saveDF.write
      .format("cn.itcast.up.model.tools.HBaseDataSource")
      .option(HBaseMeta.ZKHOSTS, meta.zkHosts)
      .option(HBaseMeta.ZKPORT, meta.zkPort)
      .option(HBaseMeta.FAMILY, meta.family)
      .option(HBaseMeta.HBASETABLE, "test30")
      .option(HBaseMeta.SELECTFIELDS, "userid,tagIds")
      .save()

  }
}
