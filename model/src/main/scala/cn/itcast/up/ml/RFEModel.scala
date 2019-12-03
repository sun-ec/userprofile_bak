package cn.itcast.up.ml

import cn.itcast.up.base.BaseModel
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Column, DataFrame, functions}

import scala.collection.immutable

/**
  * Author itcast
  * Date 2019/11/23 10:16
  * Desc 使用KMeans+RFE计算用户的活跃度模型
  * Recency:最近一次访问时间,用户最后一次访问距今时间
  * Frequency:访问频率,用户一段时间内访问的页面总次数,
  * Engagements:页面互动度,用户一段时间内访问的独立页面数,也可以定义为页面 浏览量、下载量、 视频播放数量等
  */
object RFEModel extends BaseModel{
  def main(args: Array[String]): Unit = {
    execute()
  }
  /**
    * 提供一个抽象方法由子类实现并返回标签id
    * @return
    */
  override def getTagID(): Int = 45

  /**
    * 标签计算的具体流程,应该由子类去实现
    * @param fiveRule
    * @param HBaseDF
    * @return
    */
  override def compute(fiveRule: DataFrame, HBaseDF: DataFrame): DataFrame = {
    //fiveRule.show(10,false)
    //fiveRule.printSchema()
    //HBaseDF.show(10,false)
    //HBaseDF.printSchema()
    /*
+---+----+
|id |rule|
+---+----+
|46 |1   |
|47 |2   |
|48 |3   |
|49 |4   |
+---+----+

root
 |-- id: long (nullable = false)
 |-- rule: string (nullable = true)

+--------------+-------------------------------------------------------------------+-------------------+
|global_user_id|loc_url                                                            |log_time           |
+--------------+-------------------------------------------------------------------+-------------------+
|424           |http://m.eshop.com/mobile/coupon/getCoupons.html?couponsId=3377    |2019-08-13 03:03:55|
|424           |http://m.eshop.com/?source=mobile                                  |2019-07-29 15:07:41|
|898           |http://m.eshop.com/mobile/item/11941.html                          |2019-08-14 09:23:44|
|642           |http://www.eshop.com/l/2729-2931.html                              |2019-08-11 03:20:17|
|130           |http://www.eshop.com/                                              |2019-08-12 11:59:28|
|515           |http://www.eshop.com/l/2723-0-0-1-0-0-0-0-0-0-0-0.html             |2019-07-23 14:39:25|
|274           |http://www.eshop.com/                                              |2019-07-24 15:37:12|
|772           |http://ck.eshop.com/login.html                                     |2019-07-24 07:56:49|
|189           |http://m.eshop.com/mobile/item/9673.html                           |2019-07-26 19:17:00|
|529           |http://m.eshop.com/mobile/search/_bplvbiwq_XQS75_btX_ZY1328-se.html|2019-07-25 23:18:37|
+--------------+-------------------------------------------------------------------+-------------------+
only showing top 10 rows

root
 |-- global_user_id: string (nullable = true)
 |-- loc_url: string (nullable = true)
 |-- log_time: string (nullable = true)
     */

    //0.导入隐式转换
    import spark.implicits._
    import scala.collection.JavaConversions._
    import org.apache.spark.sql.functions._
    //0.定义字符串常量
    val recencyStr = "recency"
    val frequencyStr = "frequency"
    val engagementsStr = "engagements"
    val featureStr = "feature"
    val scaleFeatureStr = "scaleFeature"
    val predictStr = "predict"

    //1.计算RFE
    //Recency:最近一次访问时间,用户最后一次访问距今时间,当前时间 - log_time
    //Frequency:访问频率,用户一段时间内访问的页面总数
    //Engagements:页面互动度,用户一段时间内访问的独立页面数,也可以定义为页面 浏览量、下载量、 视频播放数量等
    //https://blog.csdn.net/liam08/article/details/79663018
    val recencyAggColumn: Column = functions.datediff(date_sub(current_timestamp(),80),max('log_time)) as recencyStr
    val frequencyAggColumn: Column = count('loc_url) as frequencyStr
    val engagementsStrAggColumn: Column =countDistinct('loc_url) as engagementsStr

    //按照用户进行分组,计算每一个用户的RFE
    val RFEDF: DataFrame = HBaseDF.groupBy('global_user_id as "userId")
      .agg(recencyAggColumn, frequencyAggColumn, engagementsStrAggColumn)
    //RFEDF.show(10,false)
    //RFEDF.printSchema()
    /*
 +------+-------+---------+-----------+
|userId|recency|frequency|engagements|
+------+-------+---------+-----------+
|296   |18     |380      |227        |
|467   |18     |405      |267        |
|675   |18     |370      |240        |
|691   |18     |387      |244        |
|829   |18     |404      |269        |
|125   |18     |375      |246        |
|451   |18     |347      |224        |
|800   |18     |395      |242        |
|853   |18     |388      |252        |
|944   |18     |394      |252        |
+------+-------+---------+-----------+
only showing top 10 rows

root
 |-- userId: string (nullable = true)
 |-- recency: integer (nullable = true)
 |-- frequency: long (nullable = false)
 |-- engagements: long (nullable = false)
     */

    //2.使用自定义规则对数据进行归一化/打分.之后还可以继续使用SparkMLlib中的工具再对数据进行缩放到0~1之间
    // R:0-15天=5分，16-30天=4分，31-45天=3分，46-60天=2分，大于61天=1分
    // F:≥400=5分，300-399=4分，200-299=3分，100-199=2分，≤99=1分
    // E:≥250=5分，230-249=4分，210-229=3分，200-209=2分，1=1分
    val recencyScore: Column = when(col(recencyStr).between(0, 15), 5)
      .when(col(recencyStr).between(16, 30), 4)
      .when(col(recencyStr).between(31, 45), 3)
      .when(col(recencyStr).between(46, 60), 2)
      .when(col(recencyStr).gt(60), 1)//gt表示大于
      .as(recencyStr)

    val frequencyScore: Column = when(col(frequencyStr).geq(400), 5)
      .when(col(frequencyStr).between(300, 399), 4)
      .when(col(frequencyStr).between(200, 299), 3)
      .when(col(frequencyStr).between(100, 199), 2)
      .when(col(frequencyStr).leq(99), 1)//leq表示小于等于
      .as(frequencyStr)

    val engagementsScore: Column = when(col(engagementsStr).geq(250), 5)
      .when(col(engagementsStr).between(200, 249), 4)
      .when(col(engagementsStr).between(150, 199), 3)
      .when(col(engagementsStr).between(50, 149), 2)
      .when(col(engagementsStr).leq(49), 1)
      //.otherwise()//否则
      .as(engagementsStr)

    val RFEScoreDF: DataFrame = RFEDF.select('userId,recencyScore,frequencyScore,engagementsScore)
      .where('userId.isNotNull and col(recencyStr).isNotNull and col(frequencyStr).isNotNull and col(engagementsStr).isNotNull)
    //RFEScoreDF.show(10,false)
    //RFEScoreDF.printSchema()
    /*
 +------+-------+---------+-----------+
|userId|recency|frequency|engagements|
+------+-------+---------+-----------+
|296   |4      |4        |4          |
|467   |4      |5        |5          |
|675   |4      |4        |4          |
|691   |4      |4        |4          |
|829   |4      |5        |5          |
|125   |4      |4        |4          |
|451   |4      |4        |4          |
|800   |4      |4        |4          |
|853   |4      |4        |5          |
|944   |4      |4        |5          |
+------+-------+---------+-----------+
     */

    //3.特征向量化
    val VectorDF: DataFrame = new VectorAssembler()
      .setInputCols(Array(recencyStr, frequencyStr, engagementsStr))
      .setOutputCol(featureStr)
      .transform(RFEScoreDF)
    VectorDF.show(10,false)
    /*
    +------+-------+---------+-----------+-------------+
|userId|recency|frequency|engagements|feature      |
+------+-------+---------+-----------+-------------+
|296   |4      |4        |4          |[4.0,4.0,4.0]|
|467   |4      |5        |5          |[4.0,5.0,5.0]|
|675   |4      |4        |4          |[4.0,4.0,4.0]|
|691   |4      |4        |4          |[4.0,4.0,4.0]|
|829   |4      |5        |5          |[4.0,5.0,5.0]|
|125   |4      |4        |4          |[4.0,4.0,4.0]|
|451   |4      |4        |4          |[4.0,4.0,4.0]|
|800   |4      |4        |4          |[4.0,4.0,4.0]|
|853   |4      |4        |5          |[4.0,4.0,5.0]|
|944   |4      |4        |5          |[4.0,4.0,5.0]|
+------+-------+---------+-----------+-------------+
     */

    //4.创建KMeans模型并训练(这次就省略掉模型的加载/保存,K值选取了)
    val model: KMeansModel = new KMeans()
      .setFeaturesCol(featureStr)
      .setPredictionCol(predictStr)
      .setK(4)
      .setMaxIter(10)
      .setSeed(10)
      .fit(VectorDF)

    //5.预测/聚类
    val clusterDF: DataFrame = model.transform(VectorDF)
    clusterDF.show(10,false)
    /*
  +------+-------+---------+-----------+-------------+-------+
|userId|recency|frequency|engagements|feature      |predict|
+------+-------+---------+-----------+-------------+-------+
|296   |4      |4        |4          |[4.0,4.0,4.0]|0      |
|467   |4      |5        |5          |[4.0,5.0,5.0]|2      |
|675   |4      |4        |4          |[4.0,4.0,4.0]|0      |
|691   |4      |4        |4          |[4.0,4.0,4.0]|0      |
|829   |4      |5        |5          |[4.0,5.0,5.0]|2      |
|125   |4      |4        |4          |[4.0,4.0,4.0]|0      |
|451   |4      |4        |4          |[4.0,4.0,4.0]|0      |
|800   |4      |4        |4          |[4.0,4.0,4.0]|0      |
|853   |4      |4        |5          |[4.0,4.0,5.0]|1      |
|944   |4      |4        |5          |[4.0,4.0,5.0]|1      |
+------+-------+---------+-----------+-------------+-------+
     */

    //6.查看聚类中的FRE和的最大最小值
    clusterDF.groupBy(predictStr)
      .agg(max(col(recencyStr) + col(frequencyStr) + col(engagementsStr)), min(col(recencyStr) + col(frequencyStr) + col(engagementsStr)))
      .sort(col(predictStr).asc)
      .show(false)
    /*
    +-------+------------------------------------------+------------------------------------------+
|predict|max(((recency + frequency) + engagements))|min(((recency + frequency) + engagements))|
+-------+------------------------------------------+------------------------------------------+
|0      |12                                        |12                                        |
|1      |13                                        |13                                        |
|2      |14                                        |14                                        |
|3      |13                                        |13                                        |
+-------+------------------------------------------+------------------------------------------+
     */

    //7.对每一个聚类中心的FRE的值进行排序,得到排序后的集合[聚类编号,聚类中心的RFE的和]
    val sortedIndexAndRFESum: immutable.IndexedSeq[(Int, Double)] = model.clusterCenters.indices
      .map(i=> (i,model.clusterCenters(i).toArray.sum))//构建这样的集合[聚类编号,聚类中心的RFE的和]
      .sortBy(_._2).reverse//排序
    sortedIndexAndRFESum.foreach(println)
    /*
(2,14.0)
(3,13.0)
(1,13.0)
(0,12.0)
46	非常活跃		1
47	活跃		2
48	不活跃		3
49	非常不活跃		4
目标:
2 46
3 47
1 48
0 49
     */

    //8.将sortedIndexAndRFESum和5级规则进行拉链
    //+---+----+
    //|id |rule|
    //+---+----+
    //|46 |1   |
    //|47 |2   |
    //|48 |3   |
    //|49 |4   |
    //+---+----+
    val ruleList = fiveRule.as[(String, String)].map(row => {
      (row._2, row._1)
    }).collect().toList.sortBy(_._1)
    //[((聚类编号, 聚类中心的RFE的和), (rule, tagId))]
    //我们需要的是[(聚类编号,tagId)]
    val tempDF: immutable.IndexedSeq[((Int, Double), (String, String))] = sortedIndexAndRFESum.zip(ruleList)
    val predictAndTagid: immutable.IndexedSeq[(Int, String)] = tempDF.map(t=>(t._1._1,t._2._2))
    //Map[聚类编号, tagId]
    val predictAndTagidMap: Map[Int, String] = predictAndTagid.toMap
    predictAndTagidMap.foreach(println)
    /*
2 46
3 47
1 48
0 49
     */

    //9.将clusterDF中的predict换成tagId
    val predict2tag = udf((predict:Int)=>{
      predictAndTagidMap(predict)
    })

    val newDF: DataFrame = clusterDF.select('userId,predict2tag('predict) as "tagIds")
    newDF.show(10,false)
    /*
+------+------+
|userId|tagIds|
+------+------+
|   296|    49|
|   467|    46|
|   675|    49|
|   691|    49|
|   829|    46|
|   125|    49|
|   451|    49|
|   800|    49|
|   853|    48|
|   944|    48|
+------+------+
     */
    newDF
  }
}
