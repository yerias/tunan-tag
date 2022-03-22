package com.tunan.tag

object Constant {

    val HBASE:String = "hbase"
    val HBASE_ZK_QUORUM_KEY: String = "hbase.zookeeper.quorum"
    val HBASE_ZK_PORT_KEY: String = "hbase.zookeeper.property.clientPort"
    val PARAM_HBASE_ZK_QUORUM_VALUE: String = "zkHosts"
    val PARAM_HBASE_ZK_PORT_VALUE: String = "zkPort"
    val HBASE_TABLE_NAME_KEY: String = "hbaseTable"
    val HBASE_TABLE_FAMILY_KEY: String = "family"
    val HBASE_FIELDS_SEPARATOR: String = ","
    val HBASE_TABLE_SELECT_FIELDS_KEY: String = "selectFields"
    val HBASE_TABLE_ROWKEY_NAME_KEY: String = "rowKeyColumn"
    val HBASE_TABLE_FILTER_COLUMN_KEY: String = "whereFields"

    val JDBC:String = "jdbc"
    val JDBC_DRIVER_KEY:String = "driver"
    val JDBC_URL_KEY:String = "url"
    val JDBC_DBTable_KEY:String = "dbtable"
    val JDBC_USERNAME_KEY:String = "user"
    val JDBC_PASSWORD_KEY:String = "password"


    val META_RULE_SPLIT_LINE = "\\n"
    val META_RULE_SPLIT_FILET = "="


    val SPARK_HIVE_METASTORE_URL_KEY = "hive.metastore.uris"


    def main(args: Array[String]): Unit = {
    }
}