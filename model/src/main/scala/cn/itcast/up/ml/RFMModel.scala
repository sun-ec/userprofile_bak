package cn.itcast.up.ml

import cn.itcast.up.base.BaseModel
import cn.itcast.up.common.HDFSUtils
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Column, DataFrame, functions}

import scala.collection.{immutable, mutable}

/**
  * Author itcast
  * Date 2019/11/22 10:12
  * Desc 使用KMeans+RFM模型计算客户价值模型
  * Rencency:最近一次消费,最后一次订单距今时间
  * Frequency:消费频率,订单总数量
  * Monetary:消费金额,订单总金额
  * https://blog.csdn.net/liam08/article/details/79663018
  */
object RFMModel extends BaseModel{
  def main(args: Array[String]): Unit = {
    execute()
  }

  /**
    * 提供一个抽象方法由子类实现并返回标签id
    * @return
    */
  override def getTagID(): Int = 37

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
|38 |1   |
|39 |2   |
|40 |3   |
|41 |4   |
|42 |5   |
|43 |6   |
|44 |7   |
+---+----+

root
 |-- id: long (nullable = false)
 |-- rule: string (nullable = true)

+---------+-------------------+-----------+----------+
|memberId |orderSn            |orderAmount|finishTime|
+---------+-------------------+-----------+----------+
|13823431 |ts_792756751164275 |2479.45    |1564415022|
|13823431   |D14090106121770839 |2449.00   |1565687310|
|4035291  |D14090112394810659 |1099.42    |1564681801|
|4035041  |fx_787749561729045 |1999.00    |1565799378|
|13823285 |D14092120154435903 |2488.00    |1565062072|
|4034219  |D14092120155620305 |3449.00    |1563601306|
|138230939|top_810791455519102|1649.00    |1565509622|
|4035083  |D14092120161884409 |7.00       |1565731851|
|138230935|D14092120162313538 |1299.00    |1565382991|
|13823231 |D14092120162378713 |499.00     |1565677650|
+---------+-------------------+-----------+----------+
only showing top 10 rows

root
 |-- memberId: string (nullable = true)
 |-- orderSn: string (nullable = true)
 |-- orderAmount: string (nullable = true)
 |-- finishTime: string (nullable = true)
 */
    //0.导入隐式转换
    import org.apache.spark.sql.functions._
    import spark.implicits._

    //0.定义一些字符串常量,避免后续字符串拼写错误
    val recencyStr = "recency"
    val frequencyStr = "frequency"
    val monetaryStr = "monetary"
    val featureStr = "feature"
    val predictStr = "predict"

    //1.计算每个用户RFM
    //groupby memberId
    //R:最近一次消费时间距离今天的天数     --finishTime
    //F:最近一段时间的消费频次(次数) (最近一段时间由运营或产品指定)  --count(orderSn)
    //M:最近一段时间的消费总金额  --sum(orderAmount)
    //https://blog.csdn.net/liam08/article/details/79663018

    //max("finishTime"))求每个用户的最近一次的订单时间
    //from_unixtime()将字符串时间戳转为时间对象
    //datediff(结束时间,开始时间)获取两个时间的时间差--天数
    val recencyAggColumn: Column = functions.datediff(date_sub(current_timestamp(),90), from_unixtime(max('finishTime))) as recencyStr
    val frequencyAggColumn: Column = count('orderSn) as frequencyStr
    val monetaryAggColumn: Column =  sum('orderAmount) as monetaryStr

    val RFMTempDF: DataFrame = HBaseDF.groupBy('memberId)
      .agg(recencyAggColumn, frequencyAggColumn, monetaryAggColumn)
    //RFMTempDF.show(10,false)

/*
 +---------+-------+---------+------------------+
|memberId |recency|frequency|monetary          |
+---------+-------+---------+------------------+
|13822725 |7     |116      |179298.34         |
|13823083 |7     |132      |233524.17         |
|138230919|7     |125      |240061.56999999998|
|13823681 |7     |108      |169746.1          |
|4033473  |7     |142      |251930.92         |
|13822841 |7     |113      |205931.91         |
|13823153 |7     |133      |250698.57         |
|13823431 |7     |122      |180858.22         |
|4033348  |7     |145      |240173.78999999998|
|4033483  |7     |110      |157811.09999999998|
+---------+-------+---------+------------------+
*/
    //2.数据归一化(打分,打分之后再使用SparkMLlib中的归一化工具将数据缩放到0~1之间也可以)
    //R: 1-3天=5分，4-6天=4分，7-9天=3分，10-15天=2分，大于16天=1分
    //F: ≥200=5分，150-199=4分，100-149=3分，50-99=2分，1-49=1分
    //M: ≥20w=5分，10-19w=4分，5-9w=3分，1-4w=2分，<1w=1分
    val recencyScore: Column = functions.when((col(recencyStr) >= 1) && (col(recencyStr) <= 3), 5)
      .when((col(recencyStr) >= 4) && (col(recencyStr) <= 6), 4)
      .when((col(recencyStr) >= 7) && (col(recencyStr) <= 9), 3)
      .when((col(recencyStr) >= 10) && (col(recencyStr) <= 15), 2)
      .when(col(recencyStr) >= 16, 1)
      .as(recencyStr)

    val frequencyScore: Column = functions.when(col(frequencyStr) >= 200, 5)
      .when((col(frequencyStr) >= 150) && (col(frequencyStr) <= 199), 4)
      .when((col(frequencyStr) >= 100) && (col(frequencyStr) <= 149), 3)
      .when((col(frequencyStr) >= 50) && (col(frequencyStr) <= 99), 2)
      .when((col(frequencyStr) >= 1) && (col(frequencyStr) <= 49), 1)
      .as(frequencyStr)

    val monetaryScore: Column = functions.when(col(monetaryStr) >= 200000, 5)
      .when(col(monetaryStr).between(100000, 199999), 4)
      .when(col(monetaryStr).between(50000, 99999), 3)
      .when(col(monetaryStr).between(10000, 49999), 2)
      .when(col(monetaryStr) <= 9999, 1)
      .as(monetaryStr)

    val RFMScoreDF: DataFrame = RFMTempDF.select('memberId, recencyScore, frequencyScore, monetaryScore)
    //RFMScoreDF.show(10,false)
/*
+---------+-------+---------+--------+
|memberId |recency|frequency|monetary|
+---------+-------+---------+--------+
|13822725 |3      |3        |4       |
|13823083 |3      |3        |5       |
|138230919|3      |3        |5       |
|13823681 |3      |3        |4       |
|4033473  |3      |3        |5       |
|13822841 |3      |3        |5       |
|13823153 |3      |3        |5       |
|13823431 |3      |3        |4       |
|4033348  |3      |3        |5       |
|4033483  |3      |3        |4       |
+---------+-------+---------+--------+
 */

    //3.使用机器学习算法进行模型训练--首先需要对数据进行特征工程处理,我们这里需要对特征进行向量化
    val VectorDF: DataFrame = new VectorAssembler()
      .setInputCols(Array(recencyStr, frequencyStr, monetaryStr))
      .setOutputCol(featureStr)
      .transform(RFMScoreDF)
    //VectorDF.show(10,false)
/*
+---------+-------+---------+--------+-------------+
|memberId |recency|frequency|monetary|feature      |
+---------+-------+---------+--------+-------------+
|13822725 |3      |3        |4       |[3.0,3.0,4.0]|
|13823083 |3      |3        |5       |[3.0,3.0,5.0]|
|138230919|3      |3        |5       |[3.0,3.0,5.0]|
|13823681 |3      |3        |4       |[3.0,3.0,4.0]|
|4033473  |3      |3        |5       |[3.0,3.0,5.0]|
|13822841 |3      |3        |5       |[3.0,3.0,5.0]|
|13823153 |3      |3        |5       |[3.0,3.0,5.0]|
|13823431 |3      |3        |4       |[3.0,3.0,4.0]|
|4033348  |3      |3        |5       |[3.0,3.0,5.0]|
|4033483  |3      |3        |4       |[3.0,3.0,4.0]|
+---------+-------+---------+--------+-------------+
 */
    //4.选取K的值
    println("开始选取k值")
    val ks: List[Int] = List(3,4,5,6,7,8,9,10,11)
    //准备一个集合存放k对应的SSE的值
    //val表示变量不能被重新赋值
    //mutable表示该集合可变,指的是集合中的元素可以变化
    val map = mutable.Map[Int,Double]()
    //循环遍历ks,分别计算每一个k的SSE的值并放到map中
    for(k <- ks){
      val kmodel : KMeansModel =  new KMeans()
        .setFeaturesCol(featureStr)
        .setPredictionCol(predictStr)
        .setK(k) //这里先直接使用7,实际中这个应该由算法工程师训练得出K值再结合运营的实际需求选取最佳的K值
        .setMaxIter(10)
        .setSeed(10) //随机种子,方便我们测试时使用,可以保证多次运行结果一致
        .fit(VectorDF)
      //计算k的SSE
      val SSE: Double = kmodel.computeCost(VectorDF)
      map.put(k,SSE)
    }
    println("k值对应的sse的值如下:")
    map.foreach(println)
    /*
(8,13.365270018618354)
(2,218.8742481449105)
(5,22.774427527778087)
(4,49.90323450135087)
(7,16.11569736904577)
(3,61.42354755494108)
(6,20.87674069305692)
     */



    //5.模型的保存和加载
    //第一次运行的时候训练并保存模型
    //以后直接加载模型即可
    var model : KMeansModel= null
    val path = "/model/RFM"
    //判断模型路径是否存在,如果存在则直接加载模型,如果不存在则训练并保存模型
    if(HDFSUtils.getInstance().exists(path)){//存在
      println("path存在,直接加载模型使用")
      model =KMeansModel.load(path)
    }else{//不存在
      println("path不存在,需要训练模型并保存")
      //创建KMeans模型并训练
      model = new KMeans()
        .setFeaturesCol(featureStr)
        .setPredictionCol(predictStr)
        .setK(7) //这里先直接使用7,实际中这个应该由算法工程师训练得出K值再结合运营的实际需求选取最佳的K值
        .setMaxIter(10)
        .setSeed(10) //随机种子,方便我们测试时使用,可以保证多次运行结果一致
        .fit(VectorDF)
      model.save(path)
    }
    //6.预测/聚类
    val clusterDF: DataFrame = model.transform(VectorDF)
    clusterDF.show(10,false)
/*
+---------+-------+---------+--------+-------------+-------+
|memberId |recency|frequency|monetary|feature      |predict|
+---------+-------+---------+--------+-------------+-------+
|13822725 |3      |3        |4       |[3.0,3.0,4.0]|1      |
|13823083 |3      |3        |5       |[3.0,3.0,5.0]|0      |
|138230919|3      |3        |5       |[3.0,3.0,5.0]|0      |
|13823681 |3      |3        |4       |[3.0,3.0,4.0]|1      |
|4033473  |3      |3        |5       |[3.0,3.0,5.0]|0      |
|13822841 |3      |3        |5       |[3.0,3.0,5.0]|0      |
|13823153 |3      |3        |5       |[3.0,3.0,5.0]|0      |
|13823431 |3      |3        |4       |[3.0,3.0,4.0]|1      |
|4033348  |3      |3        |5       |[3.0,3.0,5.0]|0      |
|4033483  |3      |3        |4       |[3.0,3.0,4.0]|1      |
+---------+-------+---------+--------+-------------+-------+
 */

    //7.查看聚类中的FRM和的最大最小值
    clusterDF.groupBy(predictStr)
      .agg(max(col(recencyStr) + col(frequencyStr) + col(monetaryStr)), min(col(recencyStr) + col(frequencyStr) + col(monetaryStr)))
      .sort(col(predictStr).asc)
      //.show(false)
/*
+-------+---------------------------------------+---------------------------------------+
|predict|max(((recency + frequency) + monetary))|min(((recency + frequency) + monetary))|
+-------+---------------------------------------+---------------------------------------+
|0      |11                                     |10                                     |
|1      |10                                     |9                                      |
|2      |7                                      |5                                      |
|3      |3                                      |3                                      |
|4      |4                                      |4                                      |
|5      |13                                     |12                                     |
|6      |7                                      |6                                      |
+-------+---------------------------------------+---------------------------------------+
 */

/*
+---+----+
|id |rule|
+---+----+
|38 |1   |
|39 |2   |
|40 |3   |
|41 |4   |
|42 |5   |
|43 |6   |
|44 |7   |
+---+----+

38	超高价值		1
39	高价值		2
40	中上价值		3
41	中价值		4
42	中下价值		5
43	低价值		6
44	超低价值		7
 */
    //观察上面的聚类结果发现一个问题:聚类的编号和5级标签的规则顺序不匹配
    //那么我们应该要将聚类的编号和规则进行一一匹配
    //如何做?
    //可以对聚类中心的RFM的和进行排序如:
    //聚类编号   RFM和   排序
    //0         10       2
    //1         8   -->> 0
    //2         12       1
    //....

    //8.对聚类中心的RFM和的值进行排序
    //model.clusterCenters.indices获取所有聚类中心的索引编号
    //model.clusterCenters(i)根据索引取聚类中心
    //model.clusterCenters(i).toArray.sum求该聚类中心的RFM的和
    //IndexedSeq[(聚类编号索引, 聚类中心的RFM的和)]
    val indexAndRFMSum: immutable.IndexedSeq[(Int, Double)] = for(i <- model.clusterCenters.indices) yield (i,model.clusterCenters(i).toArray.sum)
    //val indexAndRFMSum2: immutable.IndexedSeq[(Int, Double)] = model.clusterCenters.indices.map(i => (i, model.clusterCenters(i).toArray.sum))
    //indexAndRFMSum.foreach(println)
    /*
(0,10.998603351955307)
(1,9.966666666666667)
(2,5.555555555555555)
(3,3.0)
(4,4.0)
(5,12.038461538461538)
(6,6.5)
     */
    println("===========================")
    //排好序的[(聚类编号索引, 聚类中心的RFM的和)]
    val sortedIndexAndRFMSum: immutable.IndexedSeq[(Int, Double)] = indexAndRFMSum.sortBy(_._2).reverse
    //sortedIndexAndRFMSum.foreach(println)
    /*
(5,12.038461538461538)
(0,10.998603351955307)
(1,9.966666666666667)
(6,6.5)
(2,5.555555555555555)
(4,4.0)
(3,3.0)
     */
    //接下来要将排好序的[(聚类编号索引, 聚类中心的RFM的和)] 和 5级标签进行匹配关联,得到如下的结果
    /*
聚类编号  tagIds
5,       38
0,       39
1,       40
6,       41
2,       42
4,       43
3,       44
     */
    println("===========================")
    //9.使用zip拉链将sortedIndexAndRFMSum和fiveRule进行拉链拼接
    val ruleList = fiveRule.as[(String, String)].map(row => {
      (row._2, row._1)
    }).collect().toList.sortBy(_._1)

    val tempZip: immutable.IndexedSeq[((Int, Double), (String, String))] = sortedIndexAndRFMSum.zip(ruleList)
    tempZip.foreach(println)
/*
((5,12.038461538461538),(1,38))
((0,10.998603351955307),(2,39))
((1,9.966666666666667),(3,40))
((6,6.5),(4,41))
((2,5.555555555555555),(5,42))
((4,4.0),(6,43))
((3,3.0),(7,44))
 */
    //IndexedSeq[(预测聚类编号, tagId)]
    val predictAndTagId: immutable.IndexedSeq[(Int, String)] = tempZip.map(t => (t._1._1, t._2._2))
    predictAndTagId.foreach(println)
    /*
(5,38)
(0,39)
(1,40)
(6,41)
(2,42)
(4,43)
(3,44)
     */
    val predictAndTagIdMap: Map[Int, String] = predictAndTagId.toMap

    //10.将clusterDF中的predict转换为TagId
    val predict2Tag = functions.udf((predict:Int)=>{
      predictAndTagIdMap(predict)
    })

    val newDF: DataFrame = clusterDF.select('memberId as "userId", predict2Tag('predict) as "tagIds")
    newDF.show(10,false)
    /*
    +---------+------+
  |userId   |tagIds|
  +---------+------+
  |13822725 |40    |
  |13823083 |39    |
  |138230919|39    |
  |13823681 |40    |
  |4033473  |39    |
  |13822841 |39    |
  |13823153 |39    |
  |13823431 |40    |
  |4033348  |39    |
  |4033483  |40    |
  +---------+------+
     */
    newDF
  }
}
