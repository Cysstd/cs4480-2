import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkPreprocess {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Pixabay Preprocess")
      .master("local[*]")     // <-- runs without HDFS
      .getOrCreate()

    import spark.implicits._

    // Load JSONL (produced by preprocess_json.py)
    val df = spark.read
      .option("multiLine", false)
      .json("data/images_raw.jsonl")

    val cleaned = df
      .filter($"views".isNotNull && $"likes".isNotNull)
      .withColumn("tag_count", size(split($"tags", ", *")))
      .withColumn("aspect_ratio", $"imageWidth" / $"imageHeight")
      .select(
        $"id",
        $"tags",
        $"tag_count",
        $"views", $"downloads", $"likes", $"comments", $"favorites",
        $"imageWidth", $"imageHeight", $"aspect_ratio"
      )

    cleaned.write
      .mode("overwrite")
      .option("header", "true")
      .csv("data/cleaned_csv")

    spark.stop()
  }
}
