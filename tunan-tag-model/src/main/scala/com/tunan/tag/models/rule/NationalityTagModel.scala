package com.tunan.tag.models.rule

import com.tunan.tag.ModelType
import com.tunan.tag.models.AbstractModel
import com.tunan.tag.utils.TagTools
import org.apache.spark.sql.DataFrame

/**
 * @Author: chb
 * @Date: 2021/4/23 17:21
 * @E-Mail:
 * @DESC: 标签模型开发：国籍标签模型
 */
class NationalityTagModel extends AbstractModel("国籍标签", ModelType.MATCH) {
  /*
  332 国籍
  333 中国 1
  334 美国 2
  335 英国 3
  336 日本 4
  337 其他 5
  */
  override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame
  = {
    // 计算标签
    val modelDF: DataFrame = TagTools.ruleMatchTag(
      businessDF, "nationality", tagDF
    )
    //modelDF.printSchema()
    //modelDF.show(100, truncate = false)
    // 返回
    modelDF
  }
}


object NationalityTagModel {
  def main(args: Array[String]): Unit = {
    val tagModel = new NationalityTagModel()
    tagModel.executeModel(332L)
  }
}
