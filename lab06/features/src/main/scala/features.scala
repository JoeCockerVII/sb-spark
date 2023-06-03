import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

object features {
  def main(args: Array[String]): Unit = {
    // Session and Config
    val spark = SparkSession.builder()
      .appName("ilya_ilyin_lab06")
      .getOrCreate()

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    import spark.implicits._

    // Read site logs (HDFS)
    val logsDataIn = spark.read.json("hdfs:///labs/laba03/weblogs.json")

    // Read users items
    val usersItemsDF = spark.read.option("path", "/user/ilya.ilyin/users-items/20200429").load()

    //------------------------------------------------------------------------------------------------------
    // 1. Explode data from logs and extract domain
    //------------------------------------------------------------------------------------------------------
    var weblogs = logsDataIn.filter(col("visits").isNotNull)
      .withColumn("data", explode(col("visits")))
      .withColumn("timestamp", col("data.timestamp"))
      .withColumn("url", col("data.url"))
      .withColumn("host", lower(callUDF("parse_url", $"url", lit("HOST"))))
      .withColumn("domain", regexp_replace($"host", "www.", ""))
      .drop("visits").drop("data").drop("url")

    // Extract and add date/time columns
    weblogs = weblogs.filter(col("domain").isNotNull)
      .withColumn("date", date_format((col("timestamp") / 1000).cast("timestamp"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("weekday", lower(date_format(col("date"), "E")))
      .withColumn("hour", date_format(col("date"), "H"))
      .withColumn("isEveningHours", when(col("hour") >= 18 and col("hour") < 24, 1).otherwise(0))
      .withColumn("isWorkHours", when(col("hour") >= 9 and col("hour") < 18, 1).otherwise(0))

    //------------------------------------------------------------------------------------------------------
    // 2. Calculate statistics pivots
    //------------------------------------------------------------------------------------------------------
    // Week days pivot
    val weblogsWeekPivot = weblogs.withColumn("weekday", concat(lit("web_day_"), col("weekday")))
      .groupBy(col("uid"), col("weekday")).agg(count("*").as("cnt"))
      .groupBy(col("uid")).pivot(col("weekday")).agg(sum(col("cnt")))
      .na.fill(0)

    // All Hours pivot
    val weblogsHoursPivot = weblogs.withColumn("hour", concat(lit("web_hour_"), col("hour")))
      .groupBy(col("uid"), col("hour")).agg(count("*").as("cnt"))
      .groupBy(col("uid")).pivot(col("hour")).agg(sum(col("cnt")))
      .na.fill(0)

    // Work and evening hours percents
    val weblogsWorkEvening = weblogs.groupBy(col("uid"))
      .agg(sum(col("isWorkHours")).as("workHours"),
        sum(col("isEveningHours")).as("eveningHours"),
        count("*").as("allHours"))
      .withColumn("web_fraction_work_hours", col("workHours") / col("allHours"))
      .withColumn("web_fraction_evening_hours", col("eveningHours") / col("allHours"))
      .select("uid","web_fraction_work_hours","web_fraction_evening_hours")

    //------------------------------------------------------------------------------------------------------
    //3. Domain features forming
    //------------------------------------------------------------------------------------------------------
    // Top 1000 ranking
    val weblogsTopRank = weblogs.groupBy(col("domain")).agg(count("*").as("count"))
      .withColumn("tmp", lit(1))
      .withColumn("rn", row_number().over(Window.partitionBy(col("tmp")).orderBy(col("count").desc)))
      .filter(col("rn") <= 1000).orderBy(col("domain"))

    weblogsTopRank.count

    // web domains pivot
    val weblogsDomainPivot = weblogs.join(weblogsTopRank, Seq("domain"), "left")
      .withColumn("count", when(col("rn").isNotNull, lit(1)).otherwise(lit(0)))
      .withColumn("domain", when(col("rn").isNotNull, col("domain").otherwise(lit("nan"))))
      .groupBy(col("uid"), col("domain")).agg(sum("count").as("count_pivot"))
      .orderBy(col("domain"))
      .groupBy(col("uid")).pivot("domain").agg(sum(col("count_pivot")))
      .na.fill(0)

    // Create domainFeatures table
    val domainFeatures = weblogsDomainPivot.select(
      col("uid"),
      array(weblogsDomainPivot.columns.filter(_ != "uid").filter(_ != "nan").map(n =>
        col("`" + n + "`")): _*).as("domain_features")
    )

    // Join all pivots with domainFeatures
    val weblogsStatFinal = domainFeatures.join(weblogsWeekPivot, Seq("uid"), "inner")
      .join(weblogsHoursPivot, Seq("uid"), "inner")
      .join(weblogsWorkEvening, Seq("uid"), "inner")

    // Join with users-items
    val finalDf = weblogsStatFinal.join(usersItemsDF, Seq("uid"), "left").na.fill(0)

    // Write result to hdfs
    finalDf
      .write
      .format("parquet")
      .mode("overwrite")
      .option("path", "/user/ilya.ilyin/features")
      .save()
  }
}
