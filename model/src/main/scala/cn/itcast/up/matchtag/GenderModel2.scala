package cn.itcast.up.matchtag

import cn.itcast.up.base.BaseModel
import org.apache.spark.sql.DataFrame

/**
  * Author itcast
  * Date 2019/11/20 10:28
  * Desc 使用抽取的BaseModel完成性别标签的计算
  */
object GenderModel2 extends BaseModel{

  def main(args: Array[String]): Unit = {
    execute()
  }

  /**
    * 提供一个抽象方法由子类实现并返回标签id
    * @return
    */
  override def getTagID(): Int = 4

  /**
    * 标签计算的具体流程,应该由子类去实现
    * @param fiveRule
    * @param HBaseDF
    * @return
    */
  override def compute(fiveRule: DataFrame, HBaseDF: DataFrame): DataFrame = {
    fiveRule.show(10)
    fiveRule.printSchema()
    HBaseDF.show(10)
    HBaseDF.printSchema()
/*
+---+----+
| id|rule|
+---+----+
|  5|   1|
|  6|   2|
+---+----+

root
 |-- id: long (nullable = false)
 |-- rule: string (nullable = true)

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
only showing top 10 rows

root
 |-- id: string (nullable = true)
 |-- gender: string (nullable = true)
 */
    import spark.implicits._
    import org.apache.spark.sql.functions._
    //Map[gender, tagId]
    val ruleMap: Map[String, String] = fiveRule.as[(String, String)].map(row => {
      (row._2, row._1)
    }).collect().toMap

    val gender2Tag = udf((gender:String)=>{
      ruleMap(gender)
    })

    //将HBase中的gender去Map中进行匹配
    val newDF: DataFrame = HBaseDF.select('id as "userId",gender2Tag('gender).as("tagIds"))
    newDF.show(10)

    newDF
  }
}
