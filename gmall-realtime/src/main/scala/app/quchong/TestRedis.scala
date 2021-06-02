package app.quchong

import redis.clients.jedis.Jedis
import util.RedisUtil
import java.util

object TestRedis {
  def main(args: Array[String]): Unit = {
    val client: Jedis = RedisUtil.getJedisClient
    client.sadd("02","aa")
    client.sadd("02","aa")
    client.sadd("02","cc")
    //    client.set("01","aa")
    val strings: util.Set[String] = client.smembers("02")

    println(strings)


  }
}
