import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

object filter {
  def main(args: Array[String]): Unit = {

    // Session and Config
    val spark = SparkSession.builder()
      .appName("ilya_ilyin_lab04")
      .getOrCreate()

    import spark.implicits._

    val topicName = spark.conf.get("spark.filter.topic_name", "lab04_input_data")
    val offset = spark.conf.get("spark.filter.offset", "earliest")
    val outputDir = spark.conf.get("spark.filter.output_dir_prefix", "/user/ilya.ilyin/visits")

    //Read from Kafka
    val inputDF: DataFrame = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("subscribe", topicName)
      .option("startingOffsets",
        if (offset.contains("earliest"))
          offset
        else {
          "{\"" + topicName + "\":{\"0\":" + offset + "}}"
        }
      ).load()

    // Convert column to string
    val valueColumnConverted: Dataset[String] = inputDF.select(col("value")).as[String]

    // Parse string to json
    var visits = spark.read.json(valueColumnConverted)

    visits = visits.withColumn("p_date", date_format((col("timestamp")./(1000)).cast("timestamp"), "yyyyMMdd"))

    // Filters
    val viewCategory = visits.filter(col("event_type").equalTo("view"))
    val buyCategory = visits.filter(col("event_type").equalTo("buy"))

    // Write to HDFS
    viewCategory
      .write
      .format("json")
      .mode("overwrite")
      .partitionBy("p_date")
      .option("path", "/user/ilya.ilyin/visits/view")
      .save()

    buyCategory
      .write
      .format("json")
      .mode("overwrite")
      .partitionBy("p_date")
      .option("path", "/user/ilya.ilyin/visits/buy")
      .save()
  }
}