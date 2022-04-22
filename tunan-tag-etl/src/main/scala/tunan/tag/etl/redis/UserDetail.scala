package tunan.tag.etl.redis

import scala.collection.mutable.ListBuffer

/**
 * @Auther: 李沅芮
 * @Date: 2022/4/22 17:03
 * @Description:
 *
 * {
 * xx1:{
 * total:888,
 * growthRate: -0.16,
 * growthRatePercent: '16%',
 * data:[6,7,1,3,9,10,5],
 * date:['04/01','04/02','04/03','04/04','04/05','04/06','04/07']
 * }
 */
case class UserDetail(
                         total: Int,
                         growthRate: Double,
                         growthRatePercent: String,
                         data: ListBuffer[Int],
                         date: ListBuffer[String]
                     )
