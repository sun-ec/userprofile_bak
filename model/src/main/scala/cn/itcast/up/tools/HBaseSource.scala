package cn.itcast.up.tools

import cn.itcast.up.bean.HBaseMeta
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, InsertableRelation, RelationProvider}

class HBaseSource extends RelationProvider with CreatableRelationProvider with DataSourceRegister with Serializable {
  /**
    * 创建读取数据Relation
    * @param sqlContext
    * @param parameters
    * @return
    */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    //解析参数
    val hbaseMeta: HBaseMeta = parseMeta(parameters)
    //返回读取数据的Relation
    new HBaseReadableRelation(sqlContext, hbaseMeta)
  }

  /**
    * 创建写入数据Relation
    * @param sqlContext
    * @param mode
    * @param parameters
    * @param data
    * @return
    */
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    //解析参数
    val hbaseMeta: HBaseMeta = parseMeta(parameters)
    val relation = new HBaseWritableRelation(sqlContext, hbaseMeta, data)
    relation.insert(data, true)
    relation
  }

  /**
    * 起个小名
    * @return
    */
  override def shortName(): String = "hbase"



  def parseMeta(params: Map[String, String]): HBaseMeta = {
    HBaseMeta(
      params.getOrElse(HBaseMeta.INTYPE, null),
      params.getOrElse(HBaseMeta.ZKHOSTS, null),
      params.getOrElse(HBaseMeta.ZKPORT, null),
      params.getOrElse(HBaseMeta.HBASETABLE, null),
      params.getOrElse(HBaseMeta.FAMILY, null),
      params.getOrElse(HBaseMeta.SELECTFIELDS, null),
      params.getOrElse(HBaseMeta.ROWKEY, null)
    )
  }


}
