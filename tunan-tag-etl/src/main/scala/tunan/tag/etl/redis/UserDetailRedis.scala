package tunan.tag.etl.redis

import scala.collection.mutable.ListBuffer

/**
 * @Auther: 李沅芮
 * @Date: 2022/4/22 17:10
 * @Description:
 */
object UserDetailRedis {

    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .master("local[*]")
            .appName("UserDetailsRedis")
            .getOrCreate()


        // spark 计算

        val userDetails = ListBuffer[UserDetail]()
        for (i <- 1 to 4) {
            userDetails += UserDetail(
                298,
                0.22,
                "22%",
                ListBuffer[Int](6, 7, 1, 3, 9, 10, 5),
                ListBuffer[String]("04/01", "04/02", "04/03", "04/04", "04/05", "04/06", "04/07"))
        }


        //        val gson: String = new Gson().toJson(userDetails)
        //        println(gson)

        // 将数据写入redis
        var jedis: Jedis = null
        try {
            jedis = RedisUtils.getJedis
            //            jedis.hset("user_profile", "user_detail", userDetails.toString)
        } catch {
            case e: Exception =>
                e.printStackTrace()
        } finally {
            jedis.close()
        }
    }
}
