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

  }
}
