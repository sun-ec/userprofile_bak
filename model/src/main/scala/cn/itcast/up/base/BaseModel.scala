package cn.itcast.up.base

import java.util.Properties

import cn.itcast.up.bean.HBaseMeta
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql._

import scala.collection.immutable

/**
  * Author itcast
  * Date 2019/11/20 9:19
  * Desc 将标签模型的共同代码进行抽取,形成BaseModel,后续的模型开发直接基础该Model即可
  */
trait BaseModel {
  //0.加载配置
  val config: Config = ConfigFactory.load()
  val url: String = config.getString("jdbc.url")
  val tableName: String = config.getString("jdbc.table")
  val sourceClass: String = config.getString("hbase.source.class")
  val zkHosts: String = config.getString("hbase.source.zkHosts")
  val zkPort: String = config.getString("hbase.source.zkPort")
  val hbaseTable: String = config.getString("hbase.source.hbaseTable")
  val family: String = config.getString("hbase.source.family")
  val selectFields: String = config.getString("hbase.source.selectFields")
  val rowKey: String = config.getString("hbase.source.rowKey")

  val hbaseMeta = HBaseMeta(
    "",
    zkHosts,
    zkPort,
    hbaseTable,
    family,
    selectFields,
    rowKey
  )
  //0.创建SparkSession
  val spark = SparkSession.builder()
    .appName("model")
    .master("local[*]")
    .config("spark.hadoop.validateOutputSpecs", "false")
    //.config("spark.local.dir","hdfs://bd001:8020/temp")
    //.config("spark.driver.memory","5g")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  //System.setProperty("HADOOP_USER_NAME","root")

  import spark.implicits._

  //下面的7个步骤中,只有第5步比较特殊,因为不同的标签计算方式可能不太一样
  //所以我们可以将第5步做成抽象方法,由子类去实现,而其他的步骤都可以在该Model中进行封装


  /**
    * execute方法将模型计算的整体流程进行了封装,子类实现BaseModel的时候只需要在main方法中调用execute方法并重写抽象方法即可
    */
  def execute()={
    //1.加载MySQL数据
    val mysqlDF:DataFrame = getMySQLDF()
    //2.获取4级标签(连接HBase的规则)
    val fourRule:Map[String,String] = getFourRule(mysqlDF)
    //3.获取5级标签
    val fiveRule:DataFrame = getFiveRule(mysqlDF)
    //4.加载HBase数据
    val HBaseDF:DataFrame = getHBaseDF(fourRule)
    //5.计算(匹配/统计/算法挖掘...)
    val newDF:DataFrame = compute(fiveRule,HBaseDF)
    //6.合并
    val result:DataFrame = merge(newDF)
    //7.保存
    save(result)
  }


  /**
    * 加载MySQL中的数据
    * @return
    */
  def getMySQLDF(): DataFrame = {
    spark.read.jdbc(url,tableName,new Properties())
  }

  /**
    * 提供一个抽象方法由子类实现并返回标签id
    * @return
    */
  def getTagID(): Int

  /**
    * 从MySQL中获取4级标签
    *
    * @param mysqlDF
    * @return
    */
  def getFourRule(mysqlDF: DataFrame): Map[String, String] = {
    //根据id查询4级规则,而id得等到子类实现的时候才知道,应该提供一个方法给子类实现
    val fourRuleDS: Dataset[Row] = mysqlDF.select('rule).where('id === getTagID())
    //inType=HBase##zkHosts=192.168.10.20##zkPort=2181##hbaseTable=tbl_users##family=detail##selectFields=id,job
    val fourRuleMap: Map[String, String] = fourRuleDS.rdd.map(row => {
      val kvStrs: Array[String] = row.getAs[String]("rule").split("##")
      val kvTuple: Array[(String, String)] = kvStrs.map(kvStr => {
        val kv: Array[String] = kvStr.split("=")
        (kv(0), kv(1))
      })
      kvTuple.toMap
    }).collect()(0)
    fourRuleMap
  }

  /**
    * 从MySQL中获取5级标签
    * @param mysqlDF
    * @return
    */
  def getFiveRule(mysqlDF: DataFrame): DataFrame = {
    mysqlDF.select('id,'rule).where('pid === getTagID())
  }

  /**
    * 根据4级标签规则从HBase中获取数据
    * @param fourRule inType=HBase##zkHosts=192.168.10.20##zkPort=2181##hbaseTable=tbl_users##family=detail##selectFields=id,job
    * @return
    */
  def getHBaseDF(fourRule: Map[String, String]): DataFrame = {
    spark.read.format("cn.itcast.up.tools.HBaseSource").options(fourRule).load()
  }

  /**
    * 标签计算的具体流程,应该由子类去实现
    * @param fiveRule
    * @param HBaseDF
    * @return
    */
  def compute(fiveRule: DataFrame, HBaseDF: DataFrame): DataFrame

  /**
    * 将这一次计算的结果和HBase中原来的结果进行合并
    * @param newDF
    * @return
    */
  def merge(newDF: DataFrame): DataFrame = {
    //1.获取原来的结果
    val oldDF: DataFrame = spark.read.format("cn.itcast.up.tools.HBaseSource")
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, hbaseMeta.hbaseTable)
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .option(HBaseMeta.SELECTFIELDS, hbaseMeta.selectFields)
      .option(HBaseMeta.ROWKEY, hbaseMeta.rowKey)
      .load()

    //2.合并oldDF和newDF
    oldDF.createOrReplaceTempView("t_old")
    newDF.createOrReplaceTempView("t_new")

    spark.udf.register("mergeTagId",(newTagsId:String,oldTagsId:String)=>{
      if(StringUtils.isBlank(newTagsId)){
        oldTagsId
      }else if(StringUtils.isBlank(oldTagsId)){
        newTagsId
      }else{
        (newTagsId.split(",") ++ oldTagsId.split(",")).toSet.mkString(",")
      }
    })

    //new 22: 10   old 22:23,30
    //result: 22: 10,23,30
    //new 22: 10  old
    //result: 22: 10
    //new   old 22:23,30
    //result
    val sql:String =
      """
        |select n.userId userId,mergeTagId(n.tagIds,o.tagIds) as tagIds from t_new n
        |left join t_old o
        |on
        |n.userId = o.userId
        |""".stripMargin
    val resutl: DataFrame = spark.sql(sql)
    resutl
  }

  /**
    * 将标签合并的结果保存到HBase
    * @param result
    */
  def save(result: DataFrame) = {
    result.show(10)
    result.write.format("cn.itcast.up.tools.HBaseSource")
      .option(HBaseMeta.ZKHOSTS, hbaseMeta.zkHosts)
      .option(HBaseMeta.ZKPORT, hbaseMeta.zkPort)
      .option(HBaseMeta.HBASETABLE, hbaseMeta.hbaseTable)
      .option(HBaseMeta.FAMILY, hbaseMeta.family)
      .option(HBaseMeta.SELECTFIELDS, hbaseMeta.selectFields)
      .option(HBaseMeta.ROWKEY, hbaseMeta.rowKey)
      .save()
  }

  /**
    * 将聚类中心索引和5级规则进行拉链,并将聚类结果中的聚类编号换成拉链结果中的tag
    * @param fiveRule 5级规则
    * @param model 模型(模型中有聚类中心的所有信息)
    * @param clusterDF 聚类结果
    * @return newDF(userId,tagIds)
    */
  def zipResult(fiveRule: DataFrame, model: KMeansModel, clusterDF: DataFrame): DataFrame = {
    import spark.implicits._
    //6.从上面的结果中可以看到聚类之后的聚类编号和5级规则顺序并不匹配
    //所以需要对各个聚类中心的psm值进行排序,得到这样的集合[聚类中心编号,psm值]
    //model.clusterCenters返回的是Array[Vector]
    //model.clusterCenters.indices返回的是所有的聚类中心的编号组成的集合
    //model.clusterCenters(i)根据索引编号取出Array[Vector]中的Vector
    //model.clusterCenters(i).toArray.sum根据索引编号取出该聚类中心(是一个Vector),再将Vector转为数组,再求和
    //IndexedSeq[(聚类中心编号, psm值)]
    val indexAndPSM: immutable.IndexedSeq[(Int, Double)] = model.clusterCenters.indices.map(i => (i, model.clusterCenters(i).toArray.sum))
    val sortedIndexAndPSM: immutable.IndexedSeq[(Int, Double)] = indexAndPSM.sortBy(_._2).reverse
    sortedIndexAndPSM.foreach(println)
    /*
(1,0.5563226557645843)
(2,0.31754213552513205)
(4,0.21281283437093323)
(0,0.1320103555777084)
(3,0.08401071578741981)
     */

    /*
+---+----+
|id |rule|
+---+----+
|51 |1   |
|52 |2   |
|53 |3   |
|54 |4   |
|55 |5   |
+---+----+

想要的结果:
(1,51)
(2,52)
(4,53)
(0,54)
(3,55)
     */


    //7.将sortedIndexAndPSM和fiveRule进行拉链
    //List[(tagId, rule)]
    val ruleList: List[(String, String)] = fiveRule.as[(String, String)].collect().toList.sortBy(_._2)
    //[((predict, psm), (tagId, rule))]
    val tempSeq: immutable.IndexedSeq[((Int, Double), (String, String))] = sortedIndexAndPSM.zip(ruleList)
    //[(predict, tagId)]
    val predictAndTagId: immutable.IndexedSeq[(Int, String)] = tempSeq.map(t => (t._1._1, t._2._1))
    val predictAndTagIdMap: Map[Int, String] = predictAndTagId.toMap
    predictAndTagIdMap.foreach(println)
    /*
(1,51)
(2,52)
(4,53)
(0,54)
(3,55)
 */
    //8.将clusterDF中的predict换成tagId
    //spark.udf.register("函数名",()=>{})

    val predict2TagId = functions.udf((predict: Int) => {
      predictAndTagIdMap(predict)
    })

    val newDF: DataFrame = clusterDF.select('userId, predict2TagId('predict).as("tagIds"))
    newDF.show(10, false)

    newDF
  }
}
