package cn.itcast.up.tools

import cn.itcast.up.bean.HBaseMeta
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation}
import org.apache.spark.sql.types.{StructField, StructType}

class HBaseWritableRelation(context: SQLContext, hbaseMeta: HBaseMeta, dataFrame: DataFrame) extends BaseRelation with InsertableRelation with Serializable {
  override def sqlContext: SQLContext = context

  override def schema: StructType = dataFrame.schema

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val list: List[StructField] = data.schema.toList
    val config = new Configuration()
    config.set("hbase.zookeeper.property.clientPort", hbaseMeta.zkPort)
    config.set("hbase.zookeeper.quorum", hbaseMeta.zkHosts)
    config.set("zookeeper.znode.parent", "/hbase-unsecure")
    config.set("mapreduce.output.fileoutputformat.outputdir", "/test")
    config.set(TableOutputFormat.OUTPUT_TABLE, hbaseMeta.hbaseTable)

    val job = Job.getInstance(config)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    data.rdd.coalesce(1)
      .map(row => {
        val put = new Put(row.getAs(hbaseMeta.rowKey).toString.getBytes)

        list.foreach(schema => {
              put.addColumn(hbaseMeta.family.getBytes, schema.name.getBytes, row.getAs(schema.name).toString.getBytes)
            })
        (new ImmutableBytesWritable, put)
      })
      .saveAsNewAPIHadoopDataset(job.getConfiguration)
  }
}
