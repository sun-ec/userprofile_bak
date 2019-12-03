package cn.itcast.up.ml

import cn.itcast.up.base.BaseModel
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{DoubleType, IntegerType}

/**
  * Author itcast
  * Date 2019/11/26 11:21
  * Desc 使用ALS基于隐语义模型的协同过滤推荐算法
  * 隐语义:指的是该算法会去挖掘用户和产品的隐藏特征
  * 协同过滤:该算法会考虑到用户和产品的交互协同过程(如评分/浏览/收藏/点赞...)并过滤出预测出的用户对产品评分较高的进行推荐
  */
object BPModel extends BaseModel{
  def main(args: Array[String]): Unit = {
    execute()
  }
  /**
    * 提供一个抽象方法由子类实现并返回标签id
    * @return
    */
  override def  getTagID(): Int = 60

  /**
    * 标签计算的具体流程,应该由子类去实现
    * @param fiveRule
    * @param HBaseDF
    * @return
    */
  override def compute(fiveRule: DataFrame, HBaseDF: DataFrame): DataFrame = {
    //HBaseDF.show(10,false)
    //HBaseDF.printSchema()
    /*
+--------------+-------------------------------------------------------------------+-------------------+
|global_user_id|loc_url                                                            |log_time           |
+--------------+-------------------------------------------------------------------+-------------------+
|424           |http://m.eshop.com/mobile/coupon/getCoupons.html?couponsId=3377    |2019-08-13 03:03:55|
|619           |http://m.eshop.com/?source=mobile                                  |2019-07-29 15:07:41|
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

    //1.解析url获取商品id
    //自定义udf完成url解析,获取出productId
    val url2productId = udf((url:String)=>{
      var productId: String = null
      if (url.contains("/product/") && url.contains(".html")) {
        val start: Int = url.indexOf("/product/")
        val end: Int = url.indexOf(".html")
        if (end > start) {
          productId = url.substring(start + 9, end)
        }
      }
      productId
    })

    val tempDF: DataFrame = HBaseDF.select('global_user_id.as("userId"),url2productId('loc_url).as("productId"))
      .filter('productId.isNotNull)//因为日志记录中的数据会包含用户访问的其他的页面,可能该条记录中没有访问产品,所以应该过滤掉
    //tempDF.show(10,false)
    //userId,productId
    /*
+------+---------+
|userId|productId|
+------+---------+
|81    |11013    |
|81   |11013    |
|302   |5353     |
|370   |9221     |
|405   |4167     |
|685   |9763     |
|733   |9501     |
|659   |11457    |
|642   |12231    |
|182   |9763     |
+------+---------+
only showing top 10 rows

81 11013 2
     */

    //2.分组并计数得到(用户id,产品id,隐式评分(浏览商品的次数))
    val tempDF2: DataFrame = tempDF.groupBy('userId, 'productId)
      .agg(count('productId) as "rating")

    //3.转换成ALS算法需要的类型(用户id-Int,产品id-Int,隐式评分(浏览商品的次数)-Double)
    //ALS only supports values in Integer range for columns userId and productId. Value null was not numeric.
    val ratingDF: Dataset[Row] = tempDF2.select('userId.cast(IntegerType), 'productId.cast(IntegerType), 'rating.cast(DoubleType))
      //考虑到我们的数据中可能有一些用户或者产品的id不是数字格式的字符串
      //那么在上一步转换的时候就失败,但是SparkSQL不会报错,而是返回null
      //而后面的机器学习算法不支持null,所以应该过滤掉,即过滤出不为null的
      .filter('userId.isNotNull && 'productId.isNotNull && 'rating.isNotNull)
    //ratingDF.show(10,false)
    //ratingDF.printSchema()
    //用户id-Int,产品id-Int,隐式评分(浏览商品的次数)-Double
/*
+------+---------+------+
|userId|productId|rating|
+------+---------+------+
|533   |11455    |1.0   |
|322   |11949    |1.0   |
|258   |7467     |1.0   |
|558   |10937    |1.0   |
|555   |10333    |1.0   |
|24    |11111    |1.0   |
|601   |5214     |2.0   |
|756   |10795    |1.0   |
|501   |12233    |3.0   |
|395   |9499     |1.0   |
+------+---------+------+
我们就得到了用户商品的评分矩阵
 */
    //4.构建ALS算法模型并训练
    val model: ALSModel = new ALS()
      .setUserCol("userId")
      .setItemCol("productId")
      .setRatingCol("rating")
      .setImplicitPrefs(true) //是否使用隐式评分,默认是false表示显示评分
      //冷启动策略支持nan和drop，采用drop //https://www.jianshu.com/p/182ae2ceb1d3
      .setColdStartStrategy("drop")
      .setRank(10) //模型中潜在因子/隐藏因子的数量,默认为10(就是k值: m行*n列  = m行*k列 X k行*n列)
      .setMaxIter(10) //最大迭代次数
      .setAlpha(1.0) //适用于ALS的隐式反馈变量的参数，其控制偏好观察中的基线置信度,默认为1.0
      .setRegParam(1.0) //指定ALS中的正则化参数（默认为1.0）
      .fit(ratingDF)

    //5.取出预测评分,给所有用户推荐他可能感兴趣的5个商品
    val result: DataFrame = model.recommendForAllUsers(5)
    //result.show(10,false)
    //result.printSchema()
    //userId,[推荐商品列表-5个商品]
    /*
+------+------------------------------------------------------------------------------------------------+
|userId|recommendations                                                                                 |
+------+------------------------------------------------------------------------------------------------+
|471   |[[10935,0.85480124], [6603,0.82611465], [9371,0.77806026], [6393,0.7115429], [6395,0.7073429]]  |
|463   |[[6603,0.73332685], [10935,0.73117584], [9371,0.7171242], [7173,0.68159044], [6395,0.6529176]]  |
|833   |[[6603,0.79149675], [10935,0.76704717], [9371,0.7628299], [7173,0.7460966], [6393,0.71079165]]  |
|496   |[[6603,0.8117078], [9371,0.7372035], [10935,0.7206242], [6393,0.71912766], [6395,0.69081223]]   |
|148   |[[6603,0.68078536], [10935,0.6766914], [9371,0.65072364], [7173,0.62549454], [6393,0.6091096]]  |
|540   |[[6603,0.7680414], [10935,0.7268602], [9371,0.7239733], [6393,0.7048858], [6395,0.6673726]]     |
|392   |[[9371,0.8268737], [10935,0.8252757], [6603,0.78230804], [10781,0.76121557], [7173,0.74036145]] |
|243   |[[10935,0.8475637], [10613,0.8446924], [6603,0.81898254], [9371,0.7813303], [6393,0.7759238]]   |
|623   |[[9371,0.8200202], [6603,0.81787753], [10935,0.7876395], [7173,0.7756662], [5394,0.7284824]]    |
|737   |[[6603,0.83411646], [9371,0.79150367], [11949,0.73539114], [10935,0.73003834], [6393,0.7108383]]|
+------+------------------------------------------------------------------------------------------------+
only showing top 10 rows

root
 |-- userId: integer (nullable = false)
 |-- recommendations: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- productId: integer (nullable = true)
 |    |    |-- rating: float (nullable = true)
     */
    //6.转换成我们需要的格式(userId,tagIds)
    val newDF: DataFrame = result.as[(Int, Array[(Int, Double)])].map(t => {
      val userId = t._1
      val tagIds: String = t._2.map(_._1).mkString(",")
      (userId, tagIds)
    }).toDF("userId", "tagIds")
    newDF.show(10,false)
    /*
+------+--------------------------+
|userId|tagIds                    |
+------+--------------------------+
|471   |10935,6603,9371,6393,6395 |
|463   |6603,10935,9371,7173,6395 |
|833   |6603,10935,9371,7173,6393 |
|496   |6603,9371,10935,6393,6395 |
|148   |6603,10935,9371,7173,6393 |
|540   |6603,10935,9371,6393,6395 |
|392   |9371,10935,6603,10781,7173|
|243   |10935,10613,6603,9371,6393|
|623   |9371,6603,10935,7173,5394 |
|737   |6603,9371,11949,10935,6393|
+------+--------------------------+
     */
    newDF
  }
}
