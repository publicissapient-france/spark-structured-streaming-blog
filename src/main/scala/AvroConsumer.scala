import com.databricks.spark.avro.SchemaConverters
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.SparkSession

object AvroConsumer {
  private val topic = "transactions"
  private val kafkaUrl = "http://localhost:9092"
  private val schemaRegistryUrl = "http://localhost:8081"

  private val schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 128)
  private val kafkaAvroDeserializer = new AvroDeserializer(schemaRegistryClient)

  private val avroSchema = schemaRegistryClient.getLatestSchemaMetadata(topic + "-value").getSchema
  private var sparkSchema = SchemaConverters.toSqlType(new Schema.Parser().parse(avroSchema))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("ConfluentConsumer")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark.udf.register("deserialize", (bytes: Array[Byte]) =>
      DeserializerWrapper.deserializer.deserialize(bytes)
    )

    val kafkaDataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaUrl)
      .option("subscribe", topic)
      .load()

    val valueDataFrame = kafkaDataFrame.selectExpr("""deserialize(value) AS message""")

    import org.apache.spark.sql.functions._

    val formattedDataFrame = valueDataFrame.select(
      from_json(col("message"), sparkSchema.dataType).alias("parsed_value"))
      .select("parsed_value.*")

    formattedDataFrame
      .writeStream
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination()
  }

  object DeserializerWrapper {
    val deserializer = kafkaAvroDeserializer
  }

  class AvroDeserializer extends AbstractKafkaAvroDeserializer {
    def this(client: SchemaRegistryClient) {
      this()
      this.schemaRegistry = client
    }

    override def deserialize(bytes: Array[Byte]): String = {
      val genericRecord = super.deserialize(bytes).asInstanceOf[GenericRecord]
      genericRecord.toString
    }
  }

}
