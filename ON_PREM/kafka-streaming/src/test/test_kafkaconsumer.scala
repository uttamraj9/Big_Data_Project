import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class KafkaConsumerTest extends AnyFunSuite with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("KafkaConsumerTest")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("Test kafka consumer JSON parsing and adding timestamp column") {

    import spark.implicits._

    // Define schema same as your kafkaconsumer.scala
    val schema = StructType(Seq(
      StructField("Transaction_ID", StringType),
      StructField("User_ID", StringType),
      StructField("Transaction_Amount", DoubleType),
      StructField("Transaction_Type", StringType),
      StructField("Timestamp", StringType),
      StructField("Account_Balance", DoubleType),
      StructField("Device_Type", StringType),
      StructField("Location", StringType),
      StructField("Merchant_Category", StringType),
      StructField("IP_Address_Flag", StringType),
      StructField("Previous_Fraudulent_Activity", StringType),
      StructField("Daily_Transaction_Count", IntegerType),
      StructField("Avg_Transaction_Amount_7d", DoubleType),
      StructField("Failed_Transaction_Count_7d", IntegerType),
      StructField("Card_Type", StringType),
      StructField("Card_Age", IntegerType),
      StructField("Transaction_Distance", DoubleType),
      StructField("Authentication_Method", StringType),
      StructField("Risk_Score", DoubleType),
      StructField("Is_Weekend", StringType)
    ))

    // Create MemoryStream for input JSON strings simulating Kafka messages
    val inputStream = MemoryStream[String]

    // Create streaming DataFrame simulating reading from Kafka
    val kafkaDF = inputStream.toDF()
      .select(from_json(col("value"), schema).alias("data"))
      .select("data.*")
      .withColumn("TimeDetails", current_timestamp())

    // Set up the query to write to memory sink for testing
    val query = kafkaDF.writeStream
      .format("memory")
      .queryName("testTable")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.Once())
      .start()

    // Add test JSON data simulating Kafka JSON message value
    val testJson =
      """{
        | "Transaction_ID": "tx001",
        | "User_ID": "user01",
        | "Transaction_Amount": 123.45,
        | "Transaction_Type": "purchase",
        | "Timestamp": "2025-06-19T10:00:00Z",
        | "Account_Balance": 1000.50,
        | "Device_Type": "mobile",
        | "Location": "NY",
        | "Merchant_Category": "retail",
        | "IP_Address_Flag": "0",
        | "Previous_Fraudulent_Activity": "no",
        | "Daily_Transaction_Count": 2,
        | "Avg_Transaction_Amount_7d": 100.25,
        | "Failed_Transaction_Count_7d": 0,
        | "Card_Type": "Visa",
        | "Card_Age": 12,
        | "Transaction_Distance": 5.5,
        | "Authentication_Method": "pin",
        | "Risk_Score": 0.3,
        | "Is_Weekend": "no"
        |}""".stripMargin

    // Add data to input stream
    inputStream.addData(testJson)

    // Wait for stream to finish
    query.awaitTermination()

    // Read from memory sink
    val resultDF = spark.sql("select * from testTable")

    // Assertions
    assert(resultDF.count() == 1)
    val row = resultDF.head()
    assert(row.getAs[String]("Transaction_ID") == "tx001")
    assert(row.getAs[Double]("Transaction_Amount") == 123.45)
    assert(row.schema.fieldNames.contains("TimeDetails"))

  }
}
