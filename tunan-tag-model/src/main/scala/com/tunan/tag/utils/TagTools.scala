package com.tunan.tag.utils

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 考虑到后续规则匹配类型标签开发，都涉及到规则匹配进行打标签，可以将抽象为函数，封装在工具类 TagTools 中以便使用
 */
object TagTools {
  /**
   *
   * @param pid
   * @return
   */
  def getTagTable(pid: Int): String = {
    // 1. 依据TagId，从MySQL读取标签数据
    // TODO: 从MySQL数据库读取标签数据（基础标签表：tbl_basic_tag），依据业务标签ID读取
    val tagTable: String =
    s"""
       |(
       |SELECT `id`,
       | `name`,
       | `rule`,
       | `level`
       |FROM `sys_tag`.`tag_basic_tag`
       |WHERE id = $pid
       |UNION
       |SELECT `id`,
       | `name`,
       | `rule`,
       | `level`
       |FROM `sys_tag`.`tag_basic_tag`
       |WHERE pid = $pid
       |ORDER BY `level` ASC, `id` ASC
       |) AS basic_tag
       |""".stripMargin

    tagTable
  }


  /**
   * 将[属性标签]数据中[规则：rule与名称：name]转换为[Map集合]
   *
   * @param tagDF 属性标签数据
   * @return Map 集合
   */
  def convertMap(tagDF: DataFrame): Map[String, String] = {
    import tagDF.sparkSession.implicits._
    tagDF
      // 获取属性标签数据
      .filter($"level" === 5)
      // 选择标签规则rule和标签Id
      .select($"rule", $"name")
      // 转换为Dataset
      .as[(String, String)]
      .rdd
      .collectAsMap().toMap
  }

  /**
   * 依据[标签业务字段的值]与[标签规则]匹配，进行打标签（userId, tagName)
   *
   * @param dataframe 标签业务数据
   * @param field     标签业务字段
   * @param tagDF     标签数据
   * @return 标签模型数据
   */
  def ruleMatchTag(dataframe: DataFrame, field: String, tagDF: DataFrame) = {
    val spark: SparkSession = dataframe.sparkSession
    import spark.implicits._
    // 1. 获取规则rule与tagId集合
    val attrTagRuleMap: Map[String, String] = convertMap(tagDF)

    // 2. 将Map集合数据广播出去
    val attrTagRuleMapBroadcast = spark.sparkContext.broadcast(attrTagRuleMap)

    // 3. 自定义UDF函数, 依据Job职业和属性标签规则进行标签化
    val field_to_tag: UserDefinedFunction = udf(
      (field: String) => attrTagRuleMapBroadcast.value(field)
    )

    // 4. 计算标签，依据业务字段值获取标签ID
    val modelDF: DataFrame = dataframe
      .select(
        $"id".as("userId"),
        field_to_tag(col(field)).as(field)
      )
    //modelDF.printSchema()
    //modelDF.show(50, truncate = false)

    // 5. 返回计算标签数据
    modelDF
  }

  /**
   * 将标签数据中属性标签规则rule拆分为范围: start, end
   *
   * @param tagDF 标签数据
   * @return 数据集DataFrame
   */
  def convertTuple(tagDF: DataFrame): DataFrame = {
    // 导入隐式转换和函数库
    import org.apache.spark.sql.functions._
    import tagDF.sparkSession.implicits._
    // 1. 自定UDF函数，解析分解属性标签的规则rule： 19500101-19591231
    val rule_to_tuple: UserDefinedFunction = udf(
      (rule: String) => {
        val Array(start, end) = rule.split("-").map(_.toInt)
        // 返回二元组
        (start, end)
      }
    )
    // 2. 获取属性标签数据，解析规则rulconvertIndexMape
    val ruleDF: DataFrame = tagDF
      .filter($"level" === 5) // 5级标签
      .select(
        $"name", //
        rule_to_tuple($"rule").as("rules") //
      )
      // 获取起始start和结束end
      .select(
        $"name", //
        $"rules._1".as("start"), //
        $"rules._2".as("end") //
      )
    //ruleDF.show(20, truncate = false)
    // 3. 返回标签规则
    ruleDF
  }

  /**
   * 将[属性标签]数据中[规则：rule与名称：name]转换[Map集合]
   *
   * @param tagDF 属性标签数据
   * @return Map 集合
   */
  /*def convertIndexMap(clusterCenters: Array[linalg.Vector], tagDF: DataFrame) = {
    import tagDF.sparkSession.implicits._
    val rulesMap: Map[String, Long] = tagDF
      //获取属性标签数据
      .filter($"level" === 5)
      //选择标签规则rule和标签ID
      .select($"rule", $"name")
      //转换为DataSet
      .as[(String, Long)]
      //转换为RDD
      .rdd
      //转换为Map集合
      .collectAsMap().toMap
    val centerIndexArray: Array[((Int, Double), Int)] = clusterCenters.zipWithIndex
      .map {
        case (vector, centerIndex) => (centerIndex, vector.toArray.sum)
      }
      .sortBy {
        case (_, rfm) => -rfm
      }
      .zipWithIndex
    //9 聚类类簇关联属性标签数据rule,对应聚类类簇与标签tagId
    val indexTagMap: Map[Int, Long] = centerIndexArray.map {
      case ((centerIndex, _), index) =>
        val tagName = rulesMap(index.toString)
        (centerIndex, tagName)
    }.toMap
    indexTagMap
  }*/
  def main(args: Array[String]): Unit = {
    println(getTagTable(2323))
  }
}
