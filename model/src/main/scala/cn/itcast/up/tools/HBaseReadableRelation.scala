package cn.itcast.up.tools

import cn.itcast.up.bean.HBaseMeta
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class HBaseReadableRelation(context: SQLContext, hbaseMeta: HBaseMeta) extends BaseRelation with TableScan with Serializable {
  override def sqlContext: SQLContext = context

  override def schema: StructType = {
    //println(hbaseMeta)
    val fields: String = hbaseMeta.selectFields
    val fieldArr: Array[StructField] = hbaseMeta.selectFields.split(",")
      .map(fieldName => {
        StructField(fieldName, StringType)
      })
    StructType(fieldArr)
  }

  //从HBase中获取数据,返回RDD
  override def buildScan(): RDD[Row] = {

    val conf = new Configuration()
    conf.set("hbase.zookeeper.property.clientPort", hbaseMeta.zkPort)
    conf.set("hbase.zookeeper.quorum", hbaseMeta.zkHosts)
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.set(TableInputFormat.INPUT_TABLE, hbaseMeta.hbaseTable)


    val sourceRDD: RDD[(ImmutableBytesWritable, Result)] = context.sparkContext.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    val resultRDD: RDD[Result] = sourceRDD.map(_._2)

    //将result=> row
    resultRDD.map(result => {

      val seq: Seq[String] = hbaseMeta.selectFields.split(",")
        .map(fieldName => {
          Bytes.toString(result.getValue(hbaseMeta.family.getBytes, fieldName.getBytes))
        }).toSeq
      Row.fromSeq(seq)
    })
  }
}
