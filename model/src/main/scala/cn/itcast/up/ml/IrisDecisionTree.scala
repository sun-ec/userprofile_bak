package cn.itcast.up.ml

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.DoubleType

/**
  * Author itcast
  * Date 2019/11/25 11:18
  * Desc 使用决策树算法对鸢尾花数据集进行分类
  */
object IrisDecisionTree {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("IrisDecisionTree")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    //1. 数据读取
    val source: DataFrame = spark.read
      .csv("file:///D:\\data\\spark\\ml\\iris_tree.csv")
      .toDF("Sepal_Length", "Sepal_Width", "Petal_Length", "Petal_Width", "Species")
      .select(
        'Sepal_Length cast DoubleType,
        'Sepal_Width cast DoubleType,
        'Petal_Length cast DoubleType,
        'Petal_Width cast DoubleType,
        'Species)
    source.show(false)
    /*
    +------------+-----------+------------+-----------+-----------+
|Sepal_Length|Sepal_Width|Petal_Length|Petal_Width|Species    |
+------------+-----------+------------+-----------+-----------+
|5.1         |3.5        |1.4         |0.2        |Iris-setosa|
|4.9         |3.0        |1.4         |0.2        |Iris-setosa|
|4.7         |3.2        |1.3         |0.2        |Iris-setosa|
|4.6         |3.1        |1.5         |0.2        |Iris-setosa|
|5.0         |3.6        |1.4         |0.2        |Iris-setosa|
|5.4         |3.9        |1.7         |0.4        |Iris-setosa|
|4.6         |3.4        |1.4         |0.3        |Iris-setosa|
|5.0         |3.4        |1.5         |0.2        |Iris-setosa|
|4.4         |2.9        |1.4         |0.2        |Iris-setosa|
|4.9         |3.1        |1.5         |0.1        |Iris-setosa|
|5.4         |3.7        |1.5         |0.2        |Iris-setosa|
|4.8         |3.4        |1.6         |0.2        |Iris-setosa|
|4.8         |3.0        |1.4         |0.1        |Iris-setosa|
|4.3         |3.0        |1.1         |0.1        |Iris-setosa|
|5.8         |4.0        |1.2         |0.2        |Iris-setosa|
|5.7         |4.4        |1.5         |0.4        |Iris-setosa|
|5.4         |3.9        |1.3         |0.4        |Iris-setosa|
|5.1         |3.5        |1.4         |0.3        |Iris-setosa|
|5.7         |3.8        |1.7         |0.3        |Iris-setosa|
|5.1         |3.8        |1.5         |0.3        |Iris-setosa|
+------------+-----------+------------+-----------+-----------+
only showing top 20 rows
     */
    //2.特征工程-标签数值化
    val stringIndexer: StringIndexer = new StringIndexer()
      .setInputCol("Species") //字符串形式
      .setOutputCol("Species_Indexer")//数值化之后的标签列


    //3.特征工程-特征向量化+归一化
    val vectorAsserbler: VectorAssembler = new VectorAssembler()
      .setInputCols(Array("Sepal_Length", "Sepal_Width", "Petal_Length", "Petal_Width"))
      .setOutputCol("features")//向量化之后的特征列

    //4.构建决策树
    val decisionTreeClassifier: DecisionTreeClassifier = new DecisionTreeClassifier()
      .setLabelCol("Species_Indexer")//数值化之后的标签列
      .setFeaturesCol("features")//向量化之后的特征列
      .setPredictionCol("predict")//预测列
      .setImpurity("gini")
      .setMaxDepth(5)


    //5.还原标签列(方便我们查看结果,才做的步骤,可以省略,如果写上,应该对预测列进行还原)
    val indexToString: IndexToString = new IndexToString()
      .setLabels(Array("Iris-versicolor","Iris-setosa","Iris-virginica"))
      .setInputCol("predict")//对预测的数值进行还原
      .setOutputCol("predict_String")//还原成字符串的标签列名

    //6.拆分数据集
    val Array(trainSet,testSet) = source.randomSplit(Array(0.8,0.2),10)

    //7.构建Pipeline并训练模型
    val pModel: PipelineModel = new Pipeline()
      .setStages(Array(stringIndexer, vectorAsserbler, decisionTreeClassifier, indexToString))
      .fit(trainSet)

    //8.预测分类
    val result: DataFrame = pModel.transform(testSet)
    result.show(false)

    //9.查看结果(查看决策过程)
    val tmodel: DecisionTreeClassificationModel = pModel.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    println(tmodel.toDebugString)//获取决策树的决策过程

    //10.评估模型
    val evalutor: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()//创建评估器
      .setLabelCol("Species_Indexer")//数值化的标签列(原来数据中有的真正的标签列)
      .setPredictionCol("predict")//和预测类别比较
      .setMetricName("accuracy")//准确率

    val acc: Double = evalutor.evaluate(result)//evaluate表示计算.评估
    println(s"准确率: ${acc}")//准确率
    println(s"错误率: ${1 - acc}")//错误率
  }
}
