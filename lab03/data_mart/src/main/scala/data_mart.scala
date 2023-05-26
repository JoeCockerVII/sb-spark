import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import java.net.{URL, URLDecoder}
import scala.util.Try

object data_mart {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
          .appName("ilya_ilyin_lab03")
          .config("spark.cassandra.connection.host", "10.0.0.31")
          .config("spark.cassandra.connection.port", "9042")
          .getOrCreate()

        // Clients data (Cassandra)
        val clientsDataIn: DataFrame = spark.read
          .format("org.apache.spark.sql.cassandra")
          .options(Map("table" -> "clients", "keyspace" -> "labdata"))
          .load()

        // Visits data (ElasticSearch)
        val visitsDataIn: DataFrame = spark.read
          .format("org.elasticsearch.spark.sql")
          .options(Map("es.read.metadata" -> "true",
              "es.nodes.wan.only" -> "true",
              "es.port" -> "9200",
              "es.nodes" -> "10.0.0.31",
              "es.net.ssl" -> "false"))
          .load("visits")

        // Site logs (HDFS)
        val logsDataIn: DataFrame = spark.read.json("hdfs:///labs/laba03/weblogs.json")

        // Site category (Postgre)
        val catsDataIn: DataFrame = spark.read
          .format("jdbc")
          .option("url", "jdbc:postgresql://10.0.0.31:5432/labdata")
          .option("dbtable", "domain_cats")
          .option("user", "ilya_ilyin")
          .option("password", "peFgYv2v")
          .option("driver", "org.postgresql.Driver")
          .load()

        // Add Age Categories
        val clients = clientsDataIn.withColumn("age_cat",
            when(col("age").between(18, 24), "18-24")
              .when(col("age").between(25, 34), "25-34")
              .when(col("age").between(35, 44), "35-44")
              .when(col("age").between(45, 54), "45-54")
              .otherwise(">=55")
        ).drop("age")

        // clients.groupBy('age_cat, 'gender).agg(count("*").as("cnt")).sort('gender, 'age_cat).show()

        // Add Shop Categories
        val visits = visitsDataIn.withColumn("shop_cat",
            lower(concat(lit("shop_"), regexp_replace(col("category"), "[ ]|[-]", "_"))))
        // Explode data from logs
        val logs = logsDataIn.filter(col("visits").isNotNull)
          .withColumn("data", explode(col("visits")))
          .withColumn("timestamp", col("data.timestamp"))
          .withColumn("url", col("data.url"))
          .drop("visits").drop("data")

        // URL decoding UDF and domain extraction
        val decodeUrlAndGetDomain = udf((url: String) => {
            Try {
                new URL(URLDecoder.decode(url, "UTF-8")).getHost
            }.getOrElse("")
        })

        // Decode URL and get only domain
        val logsWithDomain = logs.withColumn("domain", regexp_replace(
            decodeUrlAndGetDomain(col("url")), "^www.", ""))

        // Add prefix to categories
        val category = catsDataIn.withColumn("web_cat", concat(lit("web_"), col("category")))

        // Agg and pivot for logs
        val webPivotDF = logsWithDomain
          .join(category, Seq("domain"), "inner")
          .filter('uid.isNotNull)
          .filter('category.isNotNull)
          .groupBy('uid, 'web_cat).agg(count("*").as("cnt"))
          .groupBy('uid).pivot('web_cat).agg(sum('cnt))

        // Agg and pivot for visits
        val shopPivotDF = visits
          .filter('uid.isNotNull)
          .filter('category.isNotNull)
          .groupBy('uid, 'shop_cat).agg(count("*").as("cnt"))
          .groupBy('uid).pivot('shop_cat).agg(sum('cnt))

        // Join and fill null elements
        val resultDF = clients
          .join(shopPivotDF, Seq("uid"), "left")
          .join(webPivotDF, Seq("uid"), "left")

        resultDF = resultDF.na.fill(0);
        resultDF = resultDF.na.fill("0");

        // Write to DB
        resultDF.write
          .format("jdbc")
          .option("url", "jdbc:postgresql://адрес_Postgres:порт_Postgres/выходная_база_Postgres") //как в логине к личному кабинету но _ вместо .
          .option("dbtable", "выходная_таблица_Postgres")
          .option("user", "имя_фамилия")
          .option("password", "*** *****")
          .option("driver", "org.postgresql.Driver")
          .option("truncate", value = true) //позволит не терять гранты на таблицу
          .mode("overwrite") //очищает данные в таблице перед записью
          .save()
    }
}
