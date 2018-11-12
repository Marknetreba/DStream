import com.datastax.spark.connector.{SomeColumns, _}
import com.datastax.spark.connector.streaming._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStream {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[2]")
      .config("spark.cassandra.connection.host","localhost")
      .config("spark.streaming.unpersist","false")
      .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))


    import org.apache.spark.streaming.kafka010._

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


    kafkaStream.foreachRDD(rdd => if (!rdd.isEmpty()) {
      rdd.map(i=>i.value()).foreach(println)
    })


    //val data = kafkaStream.map(i => Record(i.value()))

    //data.saveToCassandra("test","data", SomeColumns("name"))

    ssc.start()
    ssc.awaitTermination()
  }
}

case class Record(name: String)
