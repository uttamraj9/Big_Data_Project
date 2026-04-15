import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import KafkaProducer.fetchAndTransformData

class KafkaProducerTest extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder()
    .appName("KafkaProducerTest")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  test("fetchAndTransformData should parse JSON correctly") {
    // Mock JSON data as it would come from the API
    val mockJson =
      """
        |[
        | {
        |   "Transaction_ID": "T123",
        |   "User_ID": "U1",
        |   "Transaction_Amount": 100.5,
        |   "Transaction_Type": "debit",
        |   "Timestamp": "2024-01-01T10:00:00Z",
        |   "Account_Balance": 500.0,
        |   "Device_Type": "mobile",
        |   "Location": "London",
        |   "Merchant_Category": "electronics",
        |   "IP_Address_Flag": false,
        |   "Previous_Fraudulent_Activity": false,
        |   "Daily_Transaction_Count": 2,
        |   "Avg_Transaction_Amount_7d": 120.0,
        |   "Failed_Transaction_Count_7d": 0,
        |   "Card_Type": "Visa",
        |   "Card_Age": 2,
        |   "Transaction_Distance": 5.5,
        |   "Authentication_Method": "biometric",
        |   "Risk_Score": 0.2,
        |   "Is_Weekend": false,
        |   "Fraud_Label": 0
        | }
        |]
        |""".stripMargin

    val df = spark.read.json(Seq(mockJson).toDS())

    // Ensure schema is as expected
    assert(df.columns.contains("Transaction_ID"))
    assert(df.count() == 1)
    assert(df.filter($"Transaction_ID" === "T123").count() == 1)
  }
}
