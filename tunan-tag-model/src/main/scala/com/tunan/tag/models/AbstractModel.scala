package com.tunan.tag.models

import com.tunan.tag.Constant._
import com.tunan.tag.ModelType
import com.tunan.tag.config.ModelConfig._
import com.tunan.tag.sql.TagSQL
import com.tunan.tag.utils.{SparkUtils, TagTools}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * @Author:
 * @Date: 2021/4/23 15:29
 * @E-Mail:
 * @DESC: 每个标签模型都有公共属性：标签名称和标签类型，所以定义抽象类 AbstractModel ，
 *        所有标签模型类继承此类，实现标签计算方法 doTag 即可。
 */
abstract class AbstractModel(modelName: String, modelType: ModelType) extends Logging {
    // 设置Spark应用程序运行的用户：root, 默认情况下为当前系统用户
    System.setProperty("user.name", "root")
    System.setProperty("HADOOP_USER_NAME", "root")
    // 变量声明
    var spark: SparkSession = _

    // 1. 初始化：构建SparkSession实例对象
    def init(isHive: Boolean): Unit = {
        spark = SparkUtils.createSparkSession(this.getClass, isHive)
    }

    /** 2. 准备标签数据：依据标签ID从MySQL数据库表tbl_basic_tag获取标签数据
     *
     * @Description 传入标签id，获取标签规则
     * @Date 16:08 2022/3/21
     * @Param [tagId]
     * @return [long]
     * */
    def getTagData(tagId: Long): DataFrame = {
        spark.read
            .format(JDBC)
            .option(JDBC_DRIVER_KEY, JDBC_DRIVER_VALUE)
            .option(JDBC_URL_KEY, JDBC_URL_VALUE)
            .option(JDBC_DBTable_KEY, TagSQL.tagTable(tagId))
            .option(JDBC_USERNAME_KEY, JDBC_USERNAME_VALUE)
            .option(JDBC_PASSWORD_KEY, JDBC_PASSWORD_VALUE)
            .load()
    }


    // 3.拆装元数据，拿到定义的规则，从目标库中读取数据
    def getBusinessData(tagDF: DataFrame): DataFrame = {

        val hbaseMeta = TagTools.convertMapToMeta(tagDF)

        /**
         * @Description 3.c. 依据标签规则中inType类型获取数据
         * @Date 16:17 2022/3/21
         * @Param [tagDF]
         * @return [org.apache.spark.sql.Dataset<org.apache.spark.sql.Row>]
         *
         *         businessDF = HBaseTools.read(
         *         spark, hbaseMeta.zkHosts, hbaseMeta.zkPort,
         *         hbaseMeta.hbaseTable,
         *         hbaseMeta.family,
         *         hbaseMeta.selectFieldNames.split(",").toSeq
         *
         *         依据条件到HBase中获取业务数据
         * */
        var businessDF: DataFrame = null
        if ("hbase".equals(hbaseMeta.inType.toLowerCase())) {
            businessDF = spark.read
                .format(HBASE)
                .option(PARAM_HBASE_ZK_QUORUM_VALUE, hbaseMeta.zkHosts)
                .option(PARAM_HBASE_ZK_PORT_VALUE, hbaseMeta.zkPort)
                .option(HBASE_TABLE_NAME_KEY, hbaseMeta.hbaseTable)
                .option(HBASE_TABLE_FAMILY_KEY, hbaseMeta.family)
                .option(HBASE_TABLE_SELECT_FIELDS_KEY, hbaseMeta.selectFieldNames)
                .option(HBASE_TABLE_FILTER_COLUMN_KEY, hbaseMeta.filterConditions)
                .load()
        } else {
            // 如果未获取到数据，直接抛出异常
            new RuntimeException("读取HBASE数据失败")
        }

        // 3.d. 返回数据
        businessDF.show()
        businessDF
    }


    /**
     * 4. 构建标签：依据业务数据和属性标签数据建立标签
     *
     * @param businessDF
     * @param tagDF
     * @return
     */
    def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame


    // 5. 保存画像标签数据至HBase表
    def saveTag(modelDF: DataFrame): Unit = {

        // 使用注册外部数据源来写入hbase
        // 写出的属性写在配置表里
        modelDF.write
            .mode(SaveMode.Overwrite)
            .format(HBASE)
            .option(HBASE_ZK_QUORUM_KEY, HBASE_ZK_HOSTS_VALUE)
            .option(HBASE_ZK_PORT_KEY, HBASE_ZK_PORT_VALUE)
            .option(HBASE_TABLE_NAME_KEY, HBASE_TABLE_NAME_VALUE)
            .option(HBASE_TABLE_FAMILY_KEY, HBASE_TABLE_FAMILY_VALUE)
            .option(HBASE_TABLE_ROWKEY_NAME_KEY, HBASE_ROWKEY_COL_VALUE)
            .save()
    }

    // 6. 关闭资源：应用结束，关闭会话实例对象
    def close(): Unit = {
        if (null != spark) spark.stop()
    }

    // 规定标签模型执行流程顺序
    def executeModel(tagId: Long, isHive: Boolean = false): Unit = {
        // a. 初始化
        init(isHive)
        try {
            // b. 获取标签数据
            val tagDF: DataFrame = getTagData(tagId)
            // cache
            tagDF.persist(StorageLevel.MEMORY_ONLY).count()
            // c. 获取业务数据
            val businessDF: DataFrame = getBusinessData(tagDF)
            // d. 计算标签
//            val modelDF: DataFrame = doTag(businessDF, tagDF)
            // e. 保存标签
//            saveTag(modelDF)
//            tagDF.unpersist()
        } catch {
            case e: Exception => e.printStackTrace()
        } finally {
            close()
        }
    }
}