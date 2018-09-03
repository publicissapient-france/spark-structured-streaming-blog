import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object SimpleConsumer {
  private val topic = "transactions_json"
  private val kafkaUrl = "http://localhost:9092"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("ConfluentConsumer")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaUrl)
      .option("subscribe", topic)
      .load()
      .selectExpr("CAST(value AS STRING) as message")

    val schema = StructType(
      Seq(
        StructField("transactiontime", LongType, true),
        StructField("transationid", StringType, true),
        StructField("clientid", StringType, true),
        StructField("transactionamount", StringType, true)
      )
    )

    import org.apache.spark.sql.functions._

    val formatted = df.select(
      from_json(col("message"), schema).alias("parsed_value"))
      .select("parsed_value.*")

    formatted
      .writeStream
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination()
  }
}
