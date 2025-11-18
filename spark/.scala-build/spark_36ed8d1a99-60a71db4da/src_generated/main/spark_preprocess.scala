

final class spark_preprocess$_ {
def args = spark_preprocess_sc.args$
def scriptPath = """spark_preprocess.sc"""
/*<script>*/
///> using scala 3.7.4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ImageJsonPreprocess {

  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: ImageJsonPreprocess <input_json_path> <output_path>")
      System.exit(1)
    }

    val inputPath = args(0)   // e.g. "hdfs:///user/you/pixabay_raw/*.json" or "file:///home/you/data/*.json"
    val outputPath = args(1)  // e.g. "hdfs:///user/you/pixabay_features"

    val spark = SparkSession.builder()
      .appName("ImageJsonPreprocess")
      // "local[*]" means use all cores on your laptop; on cluster you remove this
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // ----- 1. Read raw Pixabay JSON -----
    // If each file is a full Pixabay response with a "hits" array:
    val raw = spark.read.json(inputPath)

    // explode hits so we have one row per image
    val exploded = raw
      .withColumn("hit", explode($"hits"))
      .select("hit.*")   // flatten the hit struct

    // ----- 2. Basic cleaning + feature engineering -----
    val withFeatures = exploded
      .withColumn("tags_lower", lower($"tags"))
      .withColumn("tag_list",
        split(regexp_replace($"tags_lower", "\\s+", ""), ",")
      )
      .withColumn("tag_count", size($"tag_list"))
      .withColumn("image_pixels", $"imageWidth" * $"imageHeight")
      .withColumn("aspect_ratio",
        $"imageWidth".cast("double") / $"imageHeight".cast("double")
      )
      .withColumn("engagement",
        ($"likes" + $"comments" + $"downloads").cast("double") /
          when($"views" === 0 || $"views".isNull, lit(1.0)).otherwise($"views".cast("double"))
      )
      // simple topic flags from tags (you can adjust these later)
      .withColumn("has_animals", expr("array_contains(tag_list, 'dog') OR array_contains(tag_list, 'cat') OR array_contains(tag_list, 'animal')").cast("int"))
      .withColumn("has_people", expr("array_contains(tag_list, 'people') OR array_contains(tag_list, 'woman') OR array_contains(tag_list, 'man')").cast("int"))
      .withColumn("has_city",   expr("array_contains(tag_list, 'city') OR array_contains(tag_list, 'buildings') OR array_contains(tag_list, 'architecture')").cast("int"))

    // ----- 3. Select final columns for ML -----
    val finalDf = withFeatures.select(
      $"id".as("image_id"),
      $"user_id",
      $"type".as("image_type"),
      $"imageWidth",
      $"imageHeight",
      $"image_pixels",
      $"aspect_ratio",
      $"imageSize",
      $"views",
      $"downloads",
      $"likes",
      $"comments",
      $"collections",
      $"tag_count",
      $"tags_lower".as("tags"),
      $"has_animals",
      $"has_people",
      $"has_city",
      $"engagement"
    )

    // ----- 4. Save as Parquet or CSV (Parquet is nicer for Spark ML) -----
    finalDf.write
      .mode("overwrite")
      .parquet(outputPath)

    spark.stop()
  }
}

/*</script>*/ /*<generated>*//*</generated>*/
}

object spark_preprocess_sc {
  private var args$opt0 = Option.empty[Array[String]]
  def args$set(args: Array[String]): Unit = {
    args$opt0 = Some(args)
  }
  def args$opt: Option[Array[String]] = args$opt0
  def args$: Array[String] = args$opt.getOrElse {
    sys.error("No arguments passed to this script")
  }

  lazy val script = new spark_preprocess$_

  def main(args: Array[String]): Unit = {
    args$set(args)
    script.ImageJsonPreprocess.main(args) // hashCode to clear scalac warning about pure expression in statement position
  }
}

export spark_preprocess_sc.script as `spark_preprocess`

