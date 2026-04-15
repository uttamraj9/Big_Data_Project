import org.apache.spark.sql.SparkSession
import requests._

object kafkaproducer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Postgres to Kafka")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    var offset = 0
    val limit = 10
    val apiUrlBase = "http://18.134.163.221:30080/data"
    val kafkaServer = "ip-172-31-8-235.eu-west-2.compute.internal:9092,ip-172-31-14-3.eu-west-2.compute.internal:9092"
    val topicSampleName = "realtimeData"

    while (true) {
      try {
        val apiUrl = s"$apiUrlBase?offset=$offset&limit=$limit"
        val response = get(apiUrl)

        if (response.statusCode == 200) {
          val jsonText = response.text()
          val dfFromText = spark.read.json(Seq(jsonText).toDS)

          val messageDF = dfFromText.select(
            $"Transaction_ID",
            $"User_ID",
            $"Transaction_Amount",
            $"Transaction_Type",
            $"Timestamp",
            $"Account_Balance",
            $"Device_Type",
            $"Location",
            $"Merchant_Category",
            $"IP_Address_Flag",
            $"Previous_Fraudulent_Activity",
            $"Daily_Transaction_Count",
            $"Avg_Transaction_Amount_7d",
            $"Failed_Transaction_Count_7d",
            $"Card_Type",
            $"Card_Age",
            $"Transaction_Distance",
            $"Authentication_Method",
            $"Risk_Score",
            $"Is_Weekend",
            $"Fraud_Label"
          )

          messageDF.selectExpr("CAST(Transaction_ID AS STRING) AS key", "to_json(struct(*)) AS value")
            .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaServer)
            .option("topic", topicSampleName)
            .save()

          println(s"Loaded $limit records to Kafka topic '$topicSampleName' with offset $offset")

          // Increment offset for the next batch
          offset += limit
        } else {
          println(s"API call failed with status ${response.statusCode}")
        }
      } catch {
        case e: Exception =>
          println(s"Error in processing: ${e.getMessage}")
      }

      Thread.sleep(10000) // Wait 10 seconds before the next call
    }
  }
}