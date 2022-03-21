package com.tunan.tag.utils

import com.tunan.tag.Constant._
import com.tunan.tag.config.ModelConfig._

import java.util
import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
* @Description 创建SparkSession对象工具类
* @Date 11:35 2022/3/21
* @Param
* @return
**/
object SparkUtils {

  /**
   * 加载Spark SPARKlication默认配置文件，设置到SparkConf中
   * @param resource 资源配置文件名称
   * @return SparkConf对象
   */
  def loadConf(resource: String): SparkConf = {
    // 1. 创建SparkConf 对象
    val sparkConf = new SparkConf()
    // 2. 使用ConfigFactory加载配置文件
    val config: Config = ConfigFactory.load(resource)
    // 3. 获取加载配置信息
    val entrySet: util.Set[util.Map.Entry[String, ConfigValue]] = config.entrySet()

    // 4. 循环遍历设置属性值到SparkConf中
    import scala.collection.JavaConverters._
    entrySet.asScala.foreach{entry =>
      // 获取属性来源的文件名称
      val resourceName = entry.getValue.origin().resource()
      if(resource.equals(resourceName)){
        sparkConf.set(entry.getKey, entry.getValue.unwrapped().toString)
      }
    }
    // 5. 返回SparkConf对象
    sparkConf
  }


  /**
   * 构建SparkSession实例对象，如果是本地模式，设置master
   * @return
   */
  def createSparkSession(clazz: Class[_], isHive: Boolean = false): SparkSession = {
    // 1. 构建SparkConf对象
    val sparkConf: SparkConf = loadConf(resource = "spark.properties")
    // 2. 判断应用是否是本地模式运行，如果是设置
    if(SPARK_IS_LOCAL_VALUE){
      sparkConf.setMaster(SPARK_SPARK_MASTER_VALUE)
    }
    // 3. 创建SparkSession.Builder对象
    var builder: SparkSession.Builder = SparkSession.builder()
      .appName(clazz.getSimpleName.stripSuffix("$"))
      .config(sparkConf)

    // 4. 判断应用是否集成Hive，如果集成，设置Hive MetaStore地址
    // 如果在config.properties中设置集成Hive，表示所有SparkSPARKlication都集成 Hive；否则判断isHive，表示针对某个具体应用是否集成Hive
    if(SPARK_IS_HIVE_VALUE || isHive){
      builder = builder
        .config(SPARK_HIVE_METASTORE_URL_KEY, SPARK_HIVE_METASTORE_URL_VALUE)
        .enableHiveSupport()
    }
    // 5. 获取SparkSession对象
    val session = builder.getOrCreate()
    // 6. 返回
    session
  }
}
