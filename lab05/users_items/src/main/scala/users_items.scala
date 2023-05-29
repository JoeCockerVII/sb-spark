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

  }
}