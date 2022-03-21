package com.tunan.tag.config

import com.typesafe.config.{Config, ConfigFactory}

/**
 * @Description 读取配置文件信息config.properties，获取属性值
 * @Date 9:51 2022/3/21
 * @Param
 * @return
 **/
object ModelConfig {
	// 构建Config对象，读取配置文件
	val config: Config = ConfigFactory.load("config.properties")

	// Model Config
	lazy val MODEL_BASE_PATH: String = config.getString("tag.model.base.path")

	// MySQL Config
	lazy val JDBC_DRIVER_VALUE: String = config.getString("mysql.jdbc.driver")
	lazy val JDBC_URL_VALUE: String = config.getString("mysql.jdbc.url")
	lazy val JDBC_USERNAME_VALUE: String = config.getString("mysql.jdbc.username")
	lazy val JDBC_PASSWORD_VALUE: String = config.getString("mysql.jdbc.password")

	// Basic Tag table config
	def tagTable(tagId: Long): String = {
		s"""
		   |(
		   |SELECT `id`,
		   | `name`,
		   | `rule`,
		   | `level`
		   |FROM `sys_tag`.`tag_basic_tag`
		   |WHERE id = $tagId
		   |UNION
		   |SELECT `id`,
		   | `name`,
		   | `rule`,
		   | `level`
		   |FROM `sys_tag`.`tag_basic_tag`
		   |WHERE pid = $tagId
		   |ORDER BY `level` ASC, `id` ASC
		   |) AS basic_tag
		   |""".stripMargin
	}

	// Profile table Config
	lazy val HBASE_ZK_HOSTS_VALUE: String = config.getString("profile.hbase.zk.hosts")
	lazy val HBASE_ZK_PORT_VALUE: String = config.getString("profile.hbase.zk.port")
	lazy val HBASE_TABLE_NAME_VALUE: String = config.getString("profile.hbase.table.name")
	lazy val HBASE_TABLE_FAMILY_VALUE: String = config.getString("profile.hbase.table.family.user")

	// 作为RowKey列名称
	lazy val HBASE_ROWKEY_COL_VALUE: String = config.getString("profile.hbase.table.rowkey.col")
	// HDFS Config
	lazy val HDFS_DEFAULT_FS_VALUE: String = config.getString("fs.defaultFS")
	lazy val HDFS_FS_USER_VALUE: String = config.getString("fs.user")


	// Spark Application Local Mode
	lazy val SPARK_IS_LOCAL_VALUE: Boolean = config.getBoolean("app.is.local")
	lazy val SPARK_SPARK_MASTER_VALUE: String = config.getString("app.spark.master")


	// Spark Application With Hive
	lazy val SPARK_IS_HIVE_VALUE: Boolean = config.getBoolean("app.is.hive")
	lazy val SPARK_HIVE_METASTORE_URL_VALUE: String = config.getString("app.hive.metastore.uris")


	lazy val HIVE_METASTORE_VALUE: String = config.getString("hive_metadata")
	lazy val HIVE_WAREHOUSE_VALUE: String = config.getString("hive_warehouse")
	lazy val ZOOKEEPER_HOSTS_VALUE: String = config.getString("zookeeper_host")
	lazy val ZOOKEEPER_PORT_VALUE: String = config.getString("zookeeper_port")
}
