package com.tunan.tag.hbase

import com.tunan.tag.Constant._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.filter.{FilterList, SingleColumnValueFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
 * 自定义外部数据源：从HBase表加载数据和保存数据值HBase表
 */
class HbaseRelation(context: SQLContext,
					params: Map[String, String],
					userSchema: StructType
				   ) extends BaseRelation with TableScan with InsertableRelation with Serializable {

	/**
	 * SQLContext实例对象
	 */
	override def sqlContext: SQLContext = context

	/**
	 * DataFrame的Schema信息
	 */
	override def schema: StructType = userSchema

	/**
	 * 如何从HBase表中读取数据，返回RDD[Row]
	 */
	override def buildScan(): RDD[Row] = {
		// 1. 设置HBase中Zookeeper集群信息
		val conf: Configuration = new Configuration()
		conf.set(HBASE_ZK_QUORUM_KEY, params(PARAM_HBASE_ZK_QUORUM_VALUE))
		conf.set(HBASE_ZK_PORT_KEY, params(PARAM_HBASE_ZK_PORT_VALUE))

		// 2. 设置读HBase表的名称
		conf.set(TableInputFormat.INPUT_TABLE, params(HBASE_TABLE_NAME_KEY))

		// 3. 设置读取列簇和列名称
		val scan: Scan = new Scan()
		// 3.1. 设置列簇
		val familyBytes = Bytes.toBytes(params(HBASE_TABLE_FAMILY_KEY))
		scan.addFamily(familyBytes)
		// 3.2. 设置列名称
		val fields = params(HBASE_TABLE_SELECT_FIELDS_KEY).split(HBASE_FIELDS_SEPARATOR)
		fields.foreach { field =>
			scan.addColumn(familyBytes, Bytes.toBytes(field))
		}

		//优化 添加过滤条件格式为：modified[GE]20190601,modified[LE]20191201
		val filterConditions: String = params.getOrElse(HBASE_TABLE_FILTER_COLUMN_KEY, null)
		if (null != filterConditions && filterConditions.length > 0) {
			//构建FilterList对象
			val filterList = new FilterList()
			//依据条件语句创建Filter对象
			filterConditions.split(HBASE_FIELDS_SEPARATOR).foreach {
				filterCondition =>
					//解析Filter clause
					val condition: Condition = Condition.parseCondition(filterCondition)
					//创建SingleColumnValueFilter对象
					val filter = new SingleColumnValueFilter(
						familyBytes, //列簇
						Bytes.toBytes(condition.field), //列名
						condition.compare, //比较操作符
						Bytes.toBytes(condition.value) // 比较值
					)
					//将过滤的列加入到选择的列中
					scan.addColumn(familyBytes, Bytes.toBytes(condition.field))
					//添加到FilterList
					filterList.addFilter(filter)
			}
			scan.setFilter(filterList)
		}

		//设置scan过滤
		conf.set(
			TableInputFormat.SCAN,
			Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray)
		)

		// 4. 调用底层API，读取HBase表的数据
		val datasRDD: RDD[(ImmutableBytesWritable, Result)] =
			sqlContext.sparkContext
			  .newAPIHadoopRDD(
				  conf,
				  classOf[TableInputFormat],
				  classOf[ImmutableBytesWritable],
				  classOf[Result]
			  )

		// 5. 转换为RDD[Row]
		val rowsRDD: RDD[Row] = datasRDD.map { case (_, result) =>
			// 5.1. 列的值
			val values: Seq[String] = fields.map { field =>
				Bytes.toString(result.getValue(familyBytes,
					Bytes.toBytes(field)))
			}
			// 5.2. 生成Row对象
			Row.fromSeq(values)
		}

		// 6. 返回
		rowsRDD
	}

	/**
	 * 将数据DataFrame写入到HBase表中
	 *
	 * @param data      数据集
	 * @param overwrite 保存模式
	 */
	override def insert(data: DataFrame, overwrite: Boolean): Unit = {
		// 1. 数据转换
		val columns: Array[String] = data.columns
		val rdd: RDD[(ImmutableBytesWritable, Put)] = data.rdd.map(row => {
			val rowKey: String = row.getAs[String](params(HBASE_TABLE_ROWKEY_NAME_KEY))
			val put = new Put(Bytes.toBytes(rowKey))
			val familyBytes = Bytes.toBytes(params(HBASE_TABLE_FAMILY_KEY))

			columns.foreach { column =>
				put.addColumn(
					familyBytes, Bytes.toBytes(column), //
					Bytes.toBytes(row.getAs[String](column)) //
				)

			}
			// 返回二元组
			(new ImmutableBytesWritable, put)
		})
		// 2. 设置HBase中Zookeeper集群信息
		val conf: Configuration = new Configuration()
		conf.set(HBASE_ZK_QUORUM_KEY, params(PARAM_HBASE_ZK_QUORUM_VALUE))
		conf.set(HBASE_ZK_PORT_KEY, params(PARAM_HBASE_ZK_PORT_VALUE))

		// 3. 设置读HBase表的名称
		conf.set(TableOutputFormat.OUTPUT_TABLE, params(HBASE_TABLE_NAME_KEY))

		val job = Job.getInstance(conf)
		job.setOutputKeyClass(classOf[ImmutableBytesWritable])
		job.setOutputValueClass(classOf[Put])
		job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

		// 4. 保存数据到表
		rdd.saveAsNewAPIHadoopDataset(job.getConfiguration)
	}
}
