package cn.itcast.up.bean

case class HBaseMeta (
                       inType: String,
                       zkHosts: String,
                       zkPort: String,
                       hbaseTable: String,
                       family: String,
                       selectFields: String,
                       rowKey: String
                     )
object HBaseMeta{
  val INTYPE = "inType"
  val ZKHOSTS = "zkHosts"
  val ZKPORT = "zkPort"
  val HBASETABLE = "hbaseTable"
  val FAMILY = "family"
  val SELECTFIELDS = "selectFields"
  val ROWKEY = "rowKey"

  def apply(params: Map[String, String]): HBaseMeta = {
    HBaseMeta(
      params.getOrElse("inType", ""),
      params.getOrElse("zkHosts", ""),
      params.getOrElse("zkPort", ""),
      params.getOrElse("hbaseTable", ""),
      params.getOrElse("family", ""),
      params.getOrElse("selectFields", ""),
      params.getOrElse("rowKey", "")
    )
  }
}