package com.tunan.tag.meta

import com.tunan.tag.utils.DateUtils
import org.apache.commons.lang3.StringUtils

/**
 * @Description HBase 元数据解析存储，具体数据字段格式如下所示：
 * @Date 15:26 2022/3/21
 * @Param
 * @return
 * 依据inType类型解析为HBaseMeta，加载业务数据，核心代码如下：
 * inType=hbase
 * zkHosts=bigdata-cdh01.itcast.cn
 * zkPort=2181
 * hbaseTable=tbl_tag_users
 * family=detail
 * selectFieldNames=id,gender
 * whereCondition=modified#day#30
 * */
case class HBaseMeta(inType: String,
                     zkHosts: String,
                     zkPort: String,
                     hbaseTable: String,
                     family: String,
                     selectFieldNames: String,
                     filterConditions: String)

object HBaseMeta {
    /**
     * 将Map集合数据解析到HBaseMeta中
     *
     * @param ruleMap map集合
     * @return
     */
    def getHBaseMeta(ruleMap: Map[String, String]): HBaseMeta = {

        checkParameter(ruleMap)

        //优化加入条件过滤
        val whereCondition: String = ruleMap.getOrElse("whereCondition", null)
        //解析条件字段的值，构建where clause 语句
        /**
         * whereCondition=modified#day#30
         * whereCondition=modified#month#6
         * whereCondition=modified#year#1
         */
        var conditionStr: String = null
        if (null != whereCondition) {
            val Array(field, unit, amount) = whereCondition.split("#")
            //获取昨日日期
            val nowDate: String = DateUtils.getNow(DateUtils.SHORT_DATE_FORMAT)
            val yesterdayDate: String = DateUtils.dateCalculate(nowDate, -1)
            //依据传递的单位unit,获取最早日期时间
            val agoDate: String = unit match {
                case "day" => DateUtils.dateCalculate(yesterdayDate, -amount.toInt)
                case "month" => DateUtils.dateCalculate(yesterdayDate, -(amount.toInt * 30))
                case "year" => DateUtils.dateCalculate(yesterdayDate, -(amount.toInt * 365))
            }
            conditionStr = s"$field[GE]$agoDate, $field[LE]$yesterdayDate"
        }


        HBaseMeta(
            ruleMap("inType"),
            ruleMap("zkHosts"),
            ruleMap("zkPort"),
            ruleMap("hbaseTable"),
            ruleMap("family"),
            ruleMap("selectFieldNames"),
            conditionStr
        )
    }

    def checkParameter(ruleMap: Map[String, String]): Unit = {

        if (!ruleMap.contains("inType")) throw new RuntimeException(s"业务标签inType，没有正确传入，无法获取数据。")
        if (!ruleMap.contains("zkHosts")) throw new RuntimeException(s"业务标签zkHosts，没有正确传入，无法获取数据。")
        if (!ruleMap.contains("zkPort")) throw new RuntimeException(s"业务标签zkPort，没有正确传入，无法获取数据。")
        if (!ruleMap.contains("hbaseTable")) throw new RuntimeException(s"业务标签hbaseTable，没有正确传入，无法获取数据。")
        if (!ruleMap.contains("family")) throw new RuntimeException(s"业务标签family，没有正确传入，无法获取数据。")
        if (!ruleMap.contains("selectFieldNames")) throw new Exception(s"业务标签selectFieldNames，没有正确传入，无法获取数据。")
    }
}
