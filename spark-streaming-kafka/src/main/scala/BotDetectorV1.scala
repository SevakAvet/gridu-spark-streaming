import java.util.concurrent.TimeUnit

import com.redislabs.provider.redis.toRedisContext
import com.typesafe.config.ConfigFactory
import net.liftweb.json._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}

object BotDetectorV1 {

  object Config {
    private val config = ConfigFactory.load()

    val redisHost: String = config.getString("redis.host")
    val redisPort: Int = config.getInt("redis.port")
    val redisTtl: Int = config.getDuration("redis.ttl", TimeUnit.SECONDS).toInt
    val statisticsTtl: Int = config.getDuration("statisticsTtl", TimeUnit.SECONDS).toInt
    val kafkaHost: String = config.getString("kafka.host")
    val kafkaGroupId: String = config.getString("kafka.group-id")
    val kafkaTopic: String = config.getString("kafka.topic")
    val batchInterval: Int = config.getDuration("batchInterval", TimeUnit.SECONDS).toInt

    val eventRate: Int = config.getInt("rules.eventRate.limit")
    val clickViewRate: Int = config.getInt("rules.clickViewRate.limit")
    val categoriesRate: Int = config.getInt("rules.categoriesRate.limit")
    val window: Int = config.getDuration("rules.window", TimeUnit.SECONDS).toInt
    val watermark: Int = config.getDuration("watermark", TimeUnit.SECONDS).toInt
  }

  case class LogEvent(unix_time: Long, category_id: Long, ip: String, `type`: String)

  case class EventAggregate(ip: String, eventRate: Long, clickRate: Long, viewRate: Long, categories: Set[Long]) {
    override def toString: String = s"ip: $ip, event rate: $eventRate, clickRate: $clickRate, viewRate: $viewRate, categories: ${categories.size}"
  }

  implicit val formats: DefaultFormats.type = DefaultFormats

  def or(predicates: Seq[EventAggregate => Boolean])(eventAggregator: EventAggregate): Boolean =
    predicates.exists(predicate => predicate(eventAggregator))

  def reduceEvent(x: EventAggregate, y: EventAggregate): EventAggregate = {
    val eventRate = x.eventRate + y.eventRate
    val clickRate = x.clickRate + y.clickRate
    val viewRate = x.viewRate + y.viewRate

    val xCategories = x.categories
    val yCategories = y.categories

    // we don't want to merge 2 sets if one of them already exceeded specified limit
    if (xCategories.size > Config.categoriesRate) {
      EventAggregate(x.ip, eventRate, clickRate, viewRate, xCategories)
    } else if (yCategories.size > Config.categoriesRate) {
      EventAggregate(x.ip, eventRate, clickRate, viewRate, yCategories)
    } else {
      EventAggregate(x.ip, eventRate, clickRate, viewRate, xCategories.union(yCategories))
    }
  }

  def buildEventAggregate(x: LogEvent): EventAggregate = {
    val categories = Set(x.category_id)

    if (x.`type`.equals("click")) {
      EventAggregate(x.ip, 1, 1, 0, categories)
    } else {
      EventAggregate(x.ip, 1, 0, 1, categories)
    }
  }

  val botRules: Array[EventAggregate => Boolean] = Array[EventAggregate => Boolean](
    x => x.eventRate > Config.eventRate,
    x => x.viewRate == 0 || x.clickRate / x.viewRate > Config.clickViewRate,
    x => x.categories.size > Config.categoriesRate
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("spark")
      .master("local[*]")
      .config("spark.driver.memory", "2g")
      .config("redis.host", Config.redisHost)
      .config("redis.port", Config.redisPort)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val streamingContext = new StreamingContext(spark.sparkContext, streaming.Seconds(Config.batchInterval))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> Config.kafkaHost,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> Config.kafkaGroupId,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array(Config.kafkaTopic)
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    var offsetRanges = Array[OffsetRange]()
    val stream = kafkaStream
      .transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
      .filter(x => !x.value().isEmpty)
      .map(x => parse(x.value()).extract[LogEvent])
      .map(x => (x.ip, buildEventAggregate(x)))
      .reduceByKeyAndWindow((x: EventAggregate, y: EventAggregate) => reduceEvent(x, y),
        streaming.Seconds(Config.window), streaming.Seconds(Config.window))
      .cache()

    // put statistics into redis set 'statistics' before identifying bots
    // problem with this solution is TTL is common for all members of set
    // possible solution (made in BotDetectorV2) - put (k, v), where key has prefix,
    // e.g. 'stat_' for statistic recods and 'bot_' for bot records
    stream
      .map(x => x._2.toString)
      .foreachRDD(rdd => {
        spark.sparkContext.toRedisSET(rdd, "statistics", Config.statisticsTtl)
      })

    // identify bots by rules and put them into redis 'bots' set
    stream
      .filter(x => or(botRules)(x._2))
      .map(x => x._2.toString)
      .foreachRDD(rdd => {
        spark.sparkContext.toRedisSET(rdd, "bots", Config.redisTtl)
        kafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      })

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}