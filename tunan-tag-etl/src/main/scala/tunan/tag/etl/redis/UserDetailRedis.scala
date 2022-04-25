package tunan.tag.etl.redis

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util
import java.util.Calendar

/**
 * @Auther: 李沅芮
 * @Date: 2022/4/22 17:10
 * @Description:
 */
object UserDetailRedis {

    def getData(format: String, incDay: Integer): (String, String) = {
        val df = new SimpleDateFormat(format);
        val calendar = Calendar.getInstance();
        val end = df.format(calendar.getTime)

        calendar.add(Calendar.DATE, incDay);
        val start = df.format(calendar.getTime)

        (start, end)
    }

    def getCurrDayByYear(day:String,format: String, incDay: Long): String = {
        val ft: DateTimeFormatter = DateTimeFormatter.ofPattern(format)
        val ldt: LocalDate = LocalDate.parse(day, ft)
        val time = ldt.plusYears(incDay)
        val lastYear: String = time.format(DateTimeFormatter.ofPattern(format))
        lastYear
    }

    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .master("local[*]")
            .appName("UserDetailsRedis")
            .getOrCreate()


        // spark 计算

		val totalDetails = new util.ArrayList[TotalDetail]
		totalDetails.add(new TotalDetail(301, -0.06, "-6%", Array[Integer](6, 7, 1, 3, 9, 10, 5), Array[String]("04/01", "04/02", "04/03", "04/04", "04/05", "04/06", "04/07")))
		totalDetails.add(new TotalDetail(302, 0.19, "+19%", Array[Integer](3, 3, 3, 9, 6, 9, 6), Array[String]("04/01", "04/02", "04/03", "04/04", "04/05", "04/06", "04/07")))
		totalDetails.add(new TotalDetail(303, 0.43, "+43%", Array[Integer](1, 3, 1, 8, 7, 4, 5), Array[String]("04/01", "04/02", "04/03", "04/04", "04/05", "04/06", "04/07")))
		totalDetails.add(new TotalDetail(304, -0.18, "-39%", Array[Integer](6, 7, 6, 3, 55, 10, 7), Array[String]("04/01", "04/02", "04/03", "04/04", "04/05", "04/06", "04/07")))
		val totalDetailsJson = JSON.toJSON(totalDetails).toString


		val userDetails = new util.HashMap[String, Object]()
		userDetails.put("line",new UserDetail(Array[Integer](200, 99, 33, 66, 99, 10, 55), Array[String]("04/01", "04/02", "04/03", "04/04", "04/05", "04/06", "04/07")))
		userDetails.put("bar",new UserDetail(Array[Integer](201, 22, 33, 44, 55, 66), Array[String]("章节练习目录页", "app-课程目录页", "app首页", "我的", "题库首页", "签到首页")))

		val userDetailsJson = JSON.toJSON(userDetails).toString

		// 将数据写入redis
		var jedis: Jedis = null
		try {
			jedis = RedisUtils.getJedis

			jedis.hset("total_detail_7", "total_detail", totalDetailsJson)
			jedis.hset("user_detail_7", "112233", userDetailsJson)
		} catch {
			case e: Exception =>
				e.printStackTrace()
		} finally {
			jedis.close()
		}
	}
}
