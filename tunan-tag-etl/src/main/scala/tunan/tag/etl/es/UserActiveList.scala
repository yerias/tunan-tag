package tunan.tag.etl.es

import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.sql.sparkDatasetFunctions

/**
* @Description
* @Date 16:59 2022/4/18
* @Param
* @return
spark-submit \
--class com.wx.etl.es.UserActiveList \
--master yarn \
--deploy-mode client \
--executor-memory 3g \
--executor-cores 3 \
--num-executors 3 \
--jars $(echo `hdfs dfs -ls -C hdfs://nn:8020/spark/lib/\*.jar` | tr ' ' ',') \
/root/test/wx-bigdata-etl-1.0.0.jar 20220425
**/
object UserActiveList {
    def main(args: Array[String]): Unit = {

        val dt = args(0)

        val spark = SparkSession.builder().appName("UserActiveList")
            //.master("local[2]")
            //.config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
            //.config("spark.dynamicAllocation.enabled","false")
            //.config("es.index.auto.create","false")   // 是否自动创建index
            // .config("es.nodes","192.168.0.181")      // 测试ES节点
            .config("es.nodes.wan.only", "true") // 默认为 false，设置为 true 之后，会关闭节点的自动 discovery，只使用es.nodes声明的节点进行数据读写操作；如果你需要通过域名进行数据访问，则设置该选项为 true，否则请务必设置为 false；
            .config("es.nodes", "121.89.206.220")
            .config("es.port", "9200") // ES端口
            //            .config("es.mapping.id", "userId")     // 每个文档分配的全局id
            .config("es.batch.write.retry.count", "3") // 默认重试次数
            .config("es.batch.write.retry.wait", "5") // 每次重试等待时间单位秒
            .config("thread_pool.write.queue_size", "1000")
            .config("thread_pool.write.size", "50")
            .config("thread_pool.write.type", "fixed")
            .config("es.batch.size.bytes", "20mb")
            .config("es.batch.size.entries", "2000")
            //.config("es.http.timeout","100m")
            .enableHiveSupport()
            .getOrCreate()


        val index = "user_active_list"

        val hiveRDD = spark.sql(
            s"""
              |
              |SELECT
              |user_id,
              |user_name,
              |phone,
              |device_id,
              |'page_browse' as event_ename,
              |'页面浏览' as event_cname,
              |platfrom as platform,
              |exam_name,
              |province,
              |city,
              |act_time,
              |ip,
              |dt,
              |page_type as core_attributes,
              |source_type as additive_attribute,
              |concat_ws(' ',user_id,user_name,phone,device_id,'page_browse','页面浏览',
              |platfrom,exam_name,province,city,act_time,ip,dt,page_type,source_type)as text_attribute
              |from wx_ods.browse_clean where dt >= '${dt}' and event='page_browse'
              |
              |UNION ALL
              |
              |SELECT
              |user_id,
              |user_name,
              |phone,
              |device_id,
              |'app_click' as event_ename,
              |'app点击' as event_cname,
              |platfrom as platform,
              |exam_name,
              |province,
              |city,
              |act_time,
              |ip,
              |dt,
              |page_type as core_attributes,
              |colume_type as additive_attribute,
              |concat_ws(' ',user_id,user_name,phone,device_id,'app_click','app点击',
              |platfrom,exam_name,province,city,act_time,ip,dt,page_type,colume_type)as text_attribute
              |from wx_ods.app_click_clean where dt >= '${dt}'
              |""".stripMargin
        )

        hiveRDD.saveToEs(index)

        spark.stop()
    }
}
