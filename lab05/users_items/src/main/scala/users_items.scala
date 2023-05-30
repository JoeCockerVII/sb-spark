import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object users_items {
  def main(args: Array[String]): Unit = {

    // Session and Config
    val spark = SparkSession.builder()
      .appName("ilya_ilyin_lab05")
      .getOrCreate()

    val inputDir: String = spark.conf.get("spark.users_items.input_dir","/user/ilya.ilyin/visits")

    val outputDir: String = spark.conf.get("spark.users_items.output_dir","/user/ilya.ilyin/visits/users-items")
    val update: String = spark.conf.get("spark.users_items.update","0")

    // Read from HDFS
    val visitsView = spark.read.option("header", "true").json(inputDir + "/view")
    val visitsBuy = spark.read.option("header", "true").json(inputDir + "/buy")

    // Union of Buy and View tables
    val visits = visitsBuy.union(visitsView)

    // Find max date
    val maxDatePath = visits.select(col("p_date")).orderBy(col("p_date").desc).head()

    // Visits table modification
    val visitsModified: DataFrame =  visits
      .filter(col("uid").isNotNull)
      .filter(col("item_id").isNotNull)
      .withColumn("item_id", lower(regexp_replace(col("item_id"), "[ ]|[-]", "_")))
      .withColumn("item_modified", concat(col("event_type"), lit("_"), col("item_id")))

    // Create pivot table (users x items)
    val visitsPivot: DataFrame  = visitsModified.groupBy(col("uid"), col("item_modified")).agg(count("*").as("cnt"))
      .groupBy(col("uid")).pivot(col("item_modified")).agg(sum(col("cnt")))

    // Fill null elements
    val finalDF = visitsPivot.na.fill(0).na.fill("0");

    // Save data to HDFS
    finalDF.write
      .format("parquet")
      .option("path", outputDir + "/" + maxDatePath)
      .mode("overwrite")
      .save()
  }
}