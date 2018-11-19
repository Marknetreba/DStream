import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStream {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[2]")
      .config("spark.cassandra.connection.host", "localhost")
      .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(35))

    import org.apache.spark.streaming.kafka010._
    import spark.implicits._

    val preferredHosts = LocationStrategies.PreferConsistent

    val kafkaParams = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "dstreaming",
      "auto.offset.reset" -> "latest"
    )

    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      preferredHosts,
      ConsumerStrategies.Subscribe[String, String](Seq("data"), kafkaParams))

    val schema = new StructType()
      .add(StructField("unix_time", StringType, false))
      .add(StructField("category_id", StringType, false))
      .add(StructField("ip", StringType, false))
      .add(StructField("type", StringType, false))


    kafkaStream.map(_.value()).foreachRDD(rdd => {
      val df = rdd.toDF("value")
        .select(from_json(col("value"), schema) as "data")
        .select("data.unix_time", "data.category_id", "data.ip", "data.type")
        .toDF("time", "category", "ip", "type")
        .na.drop()
        .select("ip", "time", "type", "category")
        .groupBy("ip")
        .agg(min(from_unixtime(col("time"), "yyyy-MM-dd HH:mm:ss")) as "time_start",
          max(from_unixtime(col("time"), "yyyy-MM-dd HH:mm:ss")) as "time_end",
          collect_set("category") as "categories",
          count(when(col("type") === "click", 1)) as "clicks",
          count(when(col("type") === "view", 1)) as "views",
          count("type") as "requests")
        .withColumn("distinct_categories", size(col("categories")))
        .withColumn("duration_minutes", from_unixtime(unix_timestamp(col("time_end")).minus(unix_timestamp(col("time_start"))), "mm"))
        .withColumn("event_rate", col("requests").divide(col("duration_minutes")))
        .withColumn("categories_rate", col("distinct_categories").divide(col("duration_minutes")))
        .withColumn("views_clicks", col("views").divide(col("clicks")))
        .withColumn("bot", when(col("views_clicks") > 0.3 and col("categories_rate") > 0.5 and col("event_rate") > 100, "yes").otherwise("no"))

        .select("ip", "bot", "duration_minutes", "distinct_categories", "event_rate", "categories_rate", "views_clicks")

      df.show()

      //Save to Cassandra
            df.write
              .format("org.apache.spark.sql.cassandra")
              .mode(SaveMode.Append)
              .options(Map( "table" -> "dstream", "keyspace" -> "test"))
              .save()

    })

    ssc.start()
    ssc.awaitTermination()
  }
}