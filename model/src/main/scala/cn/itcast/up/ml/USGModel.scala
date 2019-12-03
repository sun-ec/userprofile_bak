package cn.itcast.up.ml

import cn.itcast.up.base.BaseModel
import cn.itcast.up.bean.HBaseMeta
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{Column, DataFrame, functions}

/**
  * Author itcast
  * Date 2019/11/25 14:55
  * Desc 使用决策树构建用户购物性别模型
  */
object USGModel extends BaseModel{
  def main(args: Array[String]): Unit = {
    execute()
  }
  /**
    * 提供一个抽象方法由子类实现并返回标签id
    * @return
    */
  override def getTagID(): Int = 56

  /**
    * 标签计算的具体流程,应该由子类去实现
    * @param fiveRule
    * @param HBaseDF
    * @return
    */
  override def compute(fiveRule: DataFrame, HBaseDF: DataFrame): DataFrame = {
    //fiveRule.show(10,false)
    //fiveRule.printSchema()

    //根据4级标签从HBase中查出来的实际上是商品表(订单项表)
    val goodsDF = HBaseDF
    //goodsDF.show(10,false)
    //goodsDF.printSchema()

    //0.额外再查询订单表
    val ordersDF: DataFrame = spark.read
      .format("cn.itcast.up.tools.HBaseSource")
      .option(HBaseMeta.ZKHOSTS, "bd001")
      .option(HBaseMeta.ZKPORT, "2181")
      .option(HBaseMeta.HBASETABLE, "tbl_orders")
      .option(HBaseMeta.FAMILY, "detail")
      .option(HBaseMeta.SELECTFIELDS, "memberId,orderSn")
      .load()
    //ordersDF.show(10)

    /*
 +---+----+
|id |rule|
+---+----+
|57 |0   |
|58 |1   |
|59 |-1  |
+---+----+

root
 |-- id: long (nullable = false)
 |-- rule: string (nullable = true)

//商品表(订单项表)
+----------------------+---------+-----------+
|cOrderSn              |ogColor  |productType|
+----------------------+---------+-----------+
|jd_14091818005983607  |白色       |烤箱      |
|jd_14091818005983607 |香槟金      |冰吧      |
|jd_14092012560709235  |香槟金色     |净水机  |
|rrs_15234137          |梦境极光【布朗灰】|烤箱 |
|suning_790750687478116|梦境极光【卡其金】|4K电视|
|rsq_805093707860210   |黑色       |烟灶套系  |
|jd_14090910361908941  |黑色       |智能电视  |
|jd_14091823464864679  |香槟金色     |燃气灶  |
|jd_14091817311906413  |银色       |滤芯     |
|suning_804226647488814|玫瑰金      |电饭煲  |
+----------------------+---------+-----------+
only showing top 10 rows

root
 |-- cOrderSn: string (nullable = true)
 |-- ogColor: string (nullable = true)
 |-- productType: string (nullable = true)

//订单表
+---------+-------------------+
| memberId|            orderSn|
+---------+-------------------+
| 13823431| ts_792756751164275|
|  4035167| D14090106121770839|
|  4035291| D14090112394810659|
|  4035041| fx_787749561729045|
| 13823285| D14092120154435903|
|  4034219| D14092120155620305|
|138230939|top_810791455519102|
|  4035083| D14092120161884409|
|138230935| D14092120162313538|
| 13823231| D14092120162378713|
+---------+-------------------+
only showing top 10 rows

最后我们需要的是:
+---------+----------------------+---------+-----------+
| memberId|cOrderSn              |ogColor  |productType|
+---------+----------------------+---------+-----------+
| 13823431|jd_14091818005983607  |白色       |烤箱         |
| 13823431|jd_14091818005983607 |香槟金      |冰吧         |
|  4035291|jd_14092012560709235  |香槟金色     |净水机        |
|  4035041|rrs_15234137          |梦境极光【布朗灰】|烤箱         |
| 13823285|suning_790750687478116|梦境极光【卡其金】|4K电视       |
|  4034219|rsq_805093707860210   |黑色       |烟灶套系       |
|138230939|jd_14090910361908941  |黑色       |智能电视       |
|  4035083|jd_14091823464864679  |香槟金色     |燃气灶        |
|138230935|jd_14091817311906413  |银色       |滤芯         |
| 13823231|suning_804226647488814|玫瑰金      |电饭煲        |
+---------+----------------------+---------+-----------+
     */

    //0.导入隐式转换
    import org.apache.spark.sql.functions._
    import spark.implicits._

    //1.特征选取
    //我们这里做了简化只考虑商品的颜色和商品的类型,然后使用决策树对用户购物性别进行建模/预测
    //实际中可能选取的特征会有几十个上百个(颜色/类型/大小尺寸/品牌/产地/价格.....)那么计算量会很大,可能需要进行PCA降维(主成分分析)
    //颜色ID应该来源于字典表,这里简化处理,直接使用case...when赋值
    val color: Column = functions
      .when('ogColor.equalTo("银色"), 1)
      .when('ogColor.equalTo("香槟金色"), 2)
      .when('ogColor.equalTo("黑色"), 3)
      .when('ogColor.equalTo("白色"), 4)
      .when('ogColor.equalTo("梦境极光【卡其金】"), 5)
      .when('ogColor.equalTo("梦境极光【布朗灰】"), 6)
      .when('ogColor.equalTo("粉色"), 7)
      .when('ogColor.equalTo("金属灰"), 8)
      .when('ogColor.equalTo("金色"), 9)
      .when('ogColor.equalTo("乐享金"), 10)
      .when('ogColor.equalTo("布鲁钢"), 11)
      .when('ogColor.equalTo("月光银"), 12)
      .when('ogColor.equalTo("时尚光谱【浅金棕】"), 13)
      .when('ogColor.equalTo("香槟色"), 14)
      .when('ogColor.equalTo("香槟金"), 15)
      .when('ogColor.equalTo("灰色"), 16)
      .when('ogColor.equalTo("樱花粉"), 17)
      .when('ogColor.equalTo("蓝色"), 18)
      .when('ogColor.equalTo("金属银"), 19)
      .when('ogColor.equalTo("玫瑰金"), 20)
      .otherwise(0)
      .alias("color")
    //类型ID应该来源于字典表,这里简化处理
    val productType: Column = functions
      .when('productType.equalTo("4K电视"), 9)
      .when('productType.equalTo("Haier/海尔冰箱"), 10)
      .when('productType.equalTo("Haier/海尔冰箱"), 11)
      .when('productType.equalTo("LED电视"), 12)
      .when('productType.equalTo("Leader/统帅冰箱"), 13)
      .when('productType.equalTo("冰吧"), 14)
      .when('productType.equalTo("冷柜"), 15)
      .when('productType.equalTo("净水机"), 16)
      .when('productType.equalTo("前置过滤器"), 17)
      .when('productType.equalTo("取暖电器"), 18)
      .when('productType.equalTo("吸尘器/除螨仪"), 19)
      .when('productType.equalTo("嵌入式厨电"), 20)
      .when('productType.equalTo("微波炉"), 21)
      .when('productType.equalTo("挂烫机"), 22)
      .when('productType.equalTo("料理机"), 23)
      .when('productType.equalTo("智能电视"), 24)
      .when('productType.equalTo("波轮洗衣机"), 25)
      .when('productType.equalTo("滤芯"), 26)
      .when('productType.equalTo("烟灶套系"), 27)
      .when('productType.equalTo("烤箱"), 28)
      .when('productType.equalTo("燃气灶"), 29)
      .when('productType.equalTo("燃气热水器"), 30)
      .when('productType.equalTo("电水壶/热水瓶"), 31)
      .when('productType.equalTo("电热水器"), 32)
      .when('productType.equalTo("电磁炉"), 33)
      .when('productType.equalTo("电风扇"), 34)
      .when('productType.equalTo("电饭煲"), 35)
      .when('productType.equalTo("破壁机"), 36)
      .when('productType.equalTo("空气净化器"), 37)
      .otherwise(0)
      .alias("productType")


    //2.数据标注-我们这里的标注比较简单粗糙
    //使用运营的统计数据对数据进行标注
    //运营会根据购买记录和调研信息,得出用户购物性别和购买商品的一个统计规律
    //得到的统计规律是符合现有的数据的,那么对于未来的新数据,不应该全部直接由该规律直接判断用户的购物性别
    //而应该使用算法结合运营的统计规律去挖掘分类决策依据得到一个分类模型,这样的话
    //当有新数据到来的时候就可以用该模型进行预测,而不是去直接套用该统计规律
    //训练的目的就是从已标注数据中找到规律,以后新来了一条数据,就可以进行预测
    val label: Column = functions
      .when('ogColor.equalTo("樱花粉")
        .or('ogColor.equalTo("白色"))
        .or('ogColor.equalTo("香槟色"))
        .or('ogColor.equalTo("香槟金"))
        .or('productType.equalTo("料理机"))
        .or('productType.equalTo("挂烫机"))
        .or('productType.equalTo("吸尘器/除螨仪")), 1) //女
      .otherwise(0)//男
      .alias("gender")//决策树预测label

    //
    val sourceDF: DataFrame = goodsDF.join(ordersDF, 'cOrderSn === 'orderSn)
      .select('memberId as "userId", color, productType, label)
    sourceDF.show(10,false)
/*
可以理解为处理之后的原始数据集含义为:
用户id,商品颜色,商品类型,购物性别男/女
该数据集中包含了特征列,标签列,可以使用有监督学习算法决策树进行训练
训练好之后,对于以后新来的数据就可以预测用户的购物性别是男还是女?
+---------+-----+-----------+------+
|userId   |color|productType|gender|
+---------+-----+-----------+------+
|13823535 |16   |0          |0     |
|13823535 |1    |24         |0     |
|13823535 |7    |30         |0     |
|13823391 |10   |14         |0     |
|4034493  |9    |12         |0     |
|13823683 |8    |17         |0     |
|62       |9    |15         |0     |
|4035201  |8    |12         |0     |
|13823449 |10   |0          |0     |
|138230919|12   |15         |0     |
+---------+-----+-----------+------+
 */

    //3.标签数值化(编码)
    val labelIndexer: StringIndexerModel = new StringIndexer()
      .setInputCol("gender")
      .setOutputCol("label")
      .fit(sourceDF)

    //4.特征向量化
    val featureVectorAssembler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("color", "productType"))
      .setOutputCol("features")

    //5.特征数值化(编码)
    //对特征进行索引,大于3个不同的值的特征被视为连续特征
    //VectorIndexer是对数据集特征向量中的类别(离散值)特征(index categorical features categorical features)进行编号。
    //它能够自动判断那些特征是离散值型的特征，并对他们进行编号，具体做法是通过设置一个maxCategories，
    //特征向量中某一个特征不重复取值个数小于maxCategories，则被重新编号为0～K（K<=maxCategories-1）。
    //某一个特征不重复取值个数大于maxCategories，则该特征视为连续值，不会重新编号（不会发生任何改变）
    //主要作用：提高决策树或随机森林等ML方法的分类效果
    val featureVectorIndexer: VectorIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("featureIndexed")
      .setMaxCategories(3)

    //6.构建决策树
    val decisionTreeClassifier: DecisionTreeClassifier = new DecisionTreeClassifier()
      .setFeaturesCol("featureIndexed")
      .setPredictionCol("predict")
      .setImpurity("gini") //Gini不纯度
      .setMaxDepth(5) //树的最大深度
      .setMaxBins(5)//离散化连续特征的最大划分数

    //7.还原标签列
    val labelConverter: IndexToString = new IndexToString()
      .setInputCol("label")
      .setOutputCol("labelConverted")
      .setLabels(labelIndexer.labels)

    //8.划分数据集
    val Array(traiData,testData) = sourceDF.randomSplit(Array(0.8,0.2))

    //9.构建Pipeline
    val pipeline: Pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureVectorAssembler, featureVectorIndexer, decisionTreeClassifier, labelConverter))
    val pmodel: PipelineModel = pipeline.fit(traiData)

    //10.预测
    val traiResult: DataFrame = pmodel.transform(traiData)
    val testResult: DataFrame = pmodel.transform(testData)

    //11.模型评估
    this.evaluateAUC(traiResult,testResult)

    //12.查看决策过程
    val tmodel: DecisionTreeClassificationModel = pmodel.stages(3).asInstanceOf[DecisionTreeClassificationModel]
    println(tmodel.toDebugString)//获取决策树的决策过程

    //13.查看结果
    //我们手里有的数据:
    //userId,predict
    //我们想要的数据:
    //用户id,被预测多少次,被预测为男多少次,被预测为女多少次
    //userid,total,male,female
    //1234 , 10   ,8,  2
    val tempDF:DataFrame = traiResult.union(testResult)
      .select('userId,
        when('predict === 0, 1).otherwise(0).as("male"), //计算每个用户所有订单中的男性商品的订单数
        when('predict === 1, 1).otherwise(0).as("female")) //计算每个用户所有订单中的女性商品的订单数
      .groupBy('userId)
      .agg(
        count('userId).cast(DoubleType).as("total"), //求total
        sum('male).cast(DoubleType).as("male"), //求预测为男的次数
        sum('female).cast(DoubleType).as("female") //求预测为女的次数
      )
    tempDF.show(10,false)
/*
+---------+-----+----+------+
|userId   |total|male|female|
+---------+-----+----+------+
|138230919|5.0  |3.0 |2.0   |
|4033473  |13.0 |13.0|0.0   |
|13822725 |7.0  |7.0 |0.0   |
|13823083 |17.0 |16.0|1.0   |
|13823681 |3.0  |3.0 |0.0   |
|4034923  |6.0  |5.0 |1.0   |
|4033575  |9.0  |9.0 |0.0   |
|13823431 |8.0  |7.0 |1.0   |
|4033483  |5.0  |4.0 |1.0   |
|4034191  |6.0  |4.0 |2.0   |
+---------+-----+----+------+
 */

    //最后我们想要的数据:
    //userId,tagIds
    //所以我们需要对上面的数据再进行转换,转换的依据是?
    //转换规则A:每个订单的男性商品>=80%则认定为该订单的用户为男，或女商品比例达到80%则认定为该订单的用户为女；
    //由于是家电产品，一个订单中通常只有一个商品。调整规则A为规则B：
    //转换规则B:计算每个用户近半年内所有订单中的男性商品超过60%则认定该用户为男，或近半年内所有订单中的女性品超过60%则认定该用户为女
    //fiveRule如下:
    // |-- id: long (nullable = false)
    // |-- rule: string (nullable = true)
    //+---+----+
    //|id |rule|
    //+---+----+
    //|57 |0   |
    //|58 |1   |
    //|59 |-1  |
    //+---+----+
    //现在我们手里有如上转换规则B和fiveRule,要对tempDF中的userId打上对应的tagId
    val ruleMap: Map[String, Long] = fiveRule.as[(Long,String)].map(t=>(t._2,t._1)).collect().toMap
    val gender2tag = udf((total:Double,male:Double,female:Double)=>{
      val maleRate = male / total
      val femaleRate = female / total
      if(maleRate >= 0.6){
        ruleMap("0")
      }else if(femaleRate >= 0.6){
        ruleMap("1")
      }else{
        ruleMap("-1")
      }
    })

    val newDF: DataFrame = tempDF.select('userId,gender2tag('total,'male,'female).as("tagIds"))

    newDF
  }


  def evaluateAUC(predictTrainDF: DataFrame,predictTestDF: DataFrame): Unit = {
    // 1. ACC
    val accEvaluator = new MulticlassClassificationEvaluator()
      .setPredictionCol("predict")
      .setLabelCol("label")
      .setMetricName("accuracy")//精准度

    val trainAcc: Double = accEvaluator.evaluate(predictTrainDF)
    val testAcc: Double = accEvaluator.evaluate(predictTestDF)
    println(s"训练集上的 ACC 是 : $trainAcc")
    println(s"测试集上的 ACC 是 : $testAcc")
    //训练集上的 ACC 是 : 0.7561343903359758
    //测试集上的 ACC 是 : 0.7417582417582418

    // 2. AUC
    val trainRdd: RDD[(Double, Double)] = predictTrainDF.select("label", "predict").rdd
      .map(row => (row.getAs[Double](0), row.getAs[Double](1)))
    val testRdd: RDD[(Double, Double)] = predictTestDF.select("label", "predict").rdd
      .map(row => (row.getAs[Double](0), row.getAs[Double](1)))

    val trainAUC: Double = new BinaryClassificationMetrics(trainRdd).areaUnderROC()
    val testAUC: Double = new BinaryClassificationMetrics(testRdd).areaUnderROC()
    println(s"训练集上的 AUC 是 : $trainAUC")
    println(s"测试集上的 AUC 是 : $testAUC")
    //训练集上的 AUC 是 : 0.6731303868870732
    //测试集上的 AUC 是 : 0.6482208896596393
  }
}
