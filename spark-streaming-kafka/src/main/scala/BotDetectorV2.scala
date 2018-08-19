import java.text.SimpleDateFormat

import BotDetectorV1.{Config, EventAggregate, botRules}
import com.redis.RedisClientPool
import net.liftweb.json._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ForeachWriter, SparkSession}
import org.apache.spark.streaming
import org.apache.spark.streaming._

object BotDetectorV2 {

  implicit val formats = new DefaultFormats {
    override def dateFormatter: SimpleDateFormat = new SimpleDateFormat()
  }

  def buildEventAggregate(x: LogEvent): EventAggregateWithTimestamp = {
    if (x.`type`.equals("click")) {
      EventAggregateWithTimestamp(x.unix_time, x.ip, 1, 1, 0, x.category_id)
    } else {
      EventAggregateWithTimestamp(x.unix_time, x.ip, 1, 0, 1, x.category_id)
    }
  }

  case class LogEvent(unix_time: java.sql.Timestamp, category_id: Long, ip: String, `type`: String)

  case class EventAggregateWithTimestamp(unix_time: java.sql.Timestamp, ip: String, eventRate: Long, clickRate: Long, viewRate: Long, category_id: Long) {
    override def toString: String = s"ip: $ip, event rate: $eventRate, clickRate: $clickRate, viewRate: $viewRate, categories: $category_id"
  }

  def or(predicates: Seq[EventAggregate => Boolean])(eventAggregator: EventAggregate): Boolean =
    predicates.exists(predicate => predicate(eventAggregator))

  object RedisConnection {
    lazy val pool: RedisClientPool = new RedisClientPool(Config.redisHost, Config.redisPort)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("spark")
      .master("local[*]")
      .config("spark.driver.memory", "2g")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val streamingContext = new StreamingContext(spark.sparkContext, streaming.Seconds(Config.batchInterval))

    val schema = StructType(Seq(
      StructField("unix_time", TimestampType, true),
      StructField("category_id", IntegerType, true),
      StructField("ip", StringType, true),
      StructField("type", StringType, true)
    ))

    val kafkaStream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", Config.kafkaHost)
      .option("subscribe", Config.kafkaTopic)
      .load()
      .select("value")
      .as[String]
      .filter(x => !x.isEmpty)
      .select(from_json($"value".cast(StringType), schema).as("parsed_json"))
      .select("parsed_json.*")
      .as[LogEvent]
      .map(x => buildEventAggregate(x))

    val stream = kafkaStream
      .withWatermark("unix_time", s"${Config.watermark} seconds")
      .groupBy($"ip", window($"unix_time", s"${Config.window} seconds", s"${Config.window} seconds"))
      .agg(
        count("*").as("eventRate"),
        sum("clickRate").as("clickRate"),
        sum("viewRate").as("viewRate"),
        collect_set("category_id").as("categories"))

    stream
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", value = false)
      .queryName("rate-console")
      .start

    stream
      .as[EventAggregate]
      .writeStream
      .outputMode("append")
      .foreach(new ForeachWriter[EventAggregate] {
        override def open(partitionId: Long, version: Long): Boolean = true
        override def close(errorOrNull: Throwable): Unit = {}
        override def process(value: EventAggregate): Unit = {
          RedisConnection.pool.withClient{
            client => {
              client.set(s"stat_${value.ip}", value.toString, onlyIfExists = false, com.redis.Seconds(Config.statisticsTtl))
            }
          }
        }
      })
      .queryName("statistics")
      .start

    stream
      .as[EventAggregate]
      .filter(x => or(botRules)(x))
      .writeStream
      .outputMode("append")
      .foreach(new ForeachWriter[EventAggregate] {
        override def open(partitionId: Long, version: Long): Boolean = true
        override def close(errorOrNull: Throwable): Unit = {}
        override def process(value: EventAggregate): Unit = {
          RedisConnection.pool.withClient{
            client => {
              client.set(s"bot_${value.ip}", value.toString, onlyIfExists = false, com.redis.Seconds(Config.redisTtl))
            }
          }

        }
      })
      .queryName("bots")
      .start.awaitTermination

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}