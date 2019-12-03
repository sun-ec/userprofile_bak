package cn.itcast.up.common

import java.util.Properties

import org.apache.oozie.client.OozieClient

object OozieTest {

  def upload (): Unit = {

  }

  def genProperties(): Unit = {

  }

  def main(args: Array[String]): Unit = {
    // 1. 上传文件到 HDFS 中, workflow.xml, coordinator.xml, jar
    val path = getClass.getResource("oozie/workflow.xml").getPath
    HDFSUtils.getInstance().copyFromFile(path, "/apps/tags_new/models/tags_modelid")

    // 2. 创建配置
    // 2.1. 读取配置, 这些配置可能不变, 写死的
    val prop = new Properties()
    prop.load(getClass.getResource("oozie/job.properties").openStream())
    // 2.2. 有一些参数, 必须要不同的模型计算, 有不同的值
//    prop.setProperty(...)

    // 3. 运行 Oozie 任务, oozie -oozie ... -config job.properties -run
    val client = new OozieClient("http://bd001:11000/oozie")
    // run -> submit + start
    client.run(prop)
  }
}
