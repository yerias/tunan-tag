package tunan.tag.etl.redis

import com.typesafe.config.{Config, ConfigFactory}
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * @Auther: 李沅芮
 * @Date: 2022/4/22 17:44
 * @Description:
 */
object RedisUtils {

    val conf: Config = ConfigFactory.load("redis.conf")
    private val HOSTNAME: String = conf.getString("redis.hostname")
    private val PORT: Int = conf.getInt("redis.port")
    private val MAXTOTAL: Int = conf.getInt("redis.MaxTotal")
    private val MAXWAITMILLIS: Int = conf.getInt("redis.MaxWaitMillis")
    private val MAXIDLE: Int = conf.getInt("redis.MaxIdle")
    private val TESTONBORROW: Boolean = conf.getBoolean("redis.TestOnBorrow")
    private val TESTONRETURN: Boolean = conf.getBoolean("redis.TestOnReturn")

    private val poolConfig = new JedisPoolConfig
    poolConfig.setMaxTotal(MAXTOTAL)
    poolConfig.setMaxWaitMillis(MAXWAITMILLIS)
    poolConfig.setMaxIdle(MAXIDLE)
    poolConfig.setTestOnBorrow(TESTONBORROW)
    poolConfig.setTestOnReturn(TESTONRETURN)

    private def pool = new JedisPool(poolConfig, HOSTNAME, PORT)

    // 拿到jedis客户端
    def getJedis: Jedis = pool.getResource

    def closeJedis(jedis: Jedis): Unit = {
        jedis.close()
    }

    // 测试连接
    def main(args: Array[String]): Unit = {
        println(getJedis)
    }
}
