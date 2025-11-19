// spark_preprocess.scala
// Run in Spark Shell:
// $SPARK_HOME/bin/spark-shell --master local[*]
// scala> :load spark_preprocess.scala

import org.apache.spark.sql.functions._

// ====== CONFIG – PATHS ======
val inputPath  = "hdfs://localhost:9000/user/hadoop/image_analysis/image_metadata_with_features.json"
val outputPath = "hdfs://localhost:9000/user/hadoop/image_analysis/spark_features"
// ============================

import spark.implicits._

try {
  println(s"📥 Reading enriched JSON (with OpenCV features) from: $inputPath")

  val rawDF = spark.read
    .option("multiLine", "true")
    .option("mode", "PERMISSIVE")
    .json(inputPath)

  println("📋 Raw schema:")
  rawDF.printSchema()

  println(s"🔢 Total raw records: ${rawDF.count()}")

  println("👀 Raw sample rows:")
  rawDF.show(5, truncate = false)

  // 1. Tag-based feature engineering (full chain in parentheses)
  println("🧪 Computing tag-based features...")

  val withTagFeatures = (
    rawDF
      .withColumn("tags_lower", lower(coalesce($"tags", lit(""))))
      .withColumn(
        "tag_list",
        split(regexp_replace($"tags_lower", "\\s+", ""), ",")
      )
      .withColumn(
        "tag_count",
        when($"tag_list".isNotNull, size($"tag_list")).otherwise(lit(0))
      )
      .withColumn("tag_text_len", length($"tags_lower"))
      .withColumn(
        "has_animals",
        expr(
          "array_contains(tag_list, 'dog') OR " +
          "array_contains(tag_list, 'cat') OR " +
          "array_contains(tag_list, 'animal')"
        ).cast("int")
      )
      .withColumn(
        "has_people",
        expr(
          "array_contains(tag_list, 'people') OR " +
          "array_contains(tag_list, 'woman') OR " +
          "array_contains(tag_list, 'man')"
        ).cast("int")
      )
      .withColumn(
        "has_city",
        expr(
          "array_contains(tag_list, 'city') OR " +
          "array_contains(tag_list, 'buildings') OR " +
          "array_contains(tag_list, 'architecture')"
        ).cast("int")
      )
  )

  println("📋 Tag features schema:")
  withTagFeatures.printSchema()

  println(s"🔢 Total with features: ${withTagFeatures.count()}")

  println("👀 Tag features sample:")
  withTagFeatures.select("image_id", "tags", "tags_lower", "tag_list", "tag_count", "has_animals").show(5, false)

  // 2. Select final columns for ML (wrapped in parentheses)
  println("📦 Selecting final ML feature columns...")

  val finalDf = (
    withTagFeatures.select(
      $"label".cast("double").as("label"),
      $"image_id",
      $"keyword",
      $"source_url",
      $"user",
      $"tags_lower".as("tags"),
      $"views",
      $"downloads",
      $"tag_count",
      $"tag_text_len",
      $"has_animals",
      $"has_people",
      $"has_city",
      $"r_mean", $"g_mean", $"b_mean",
      $"r_std", $"g_std", $"b_std",
      $"gray_mean", $"gray_std",
      $"laplacian_var",
      $"edge_density",
      $"hog_0", $"hog_1", $"hog_2",
      $"hog_3", $"hog_4", $"hog_5",
      $"hog_6", $"hog_7", $"hog_8"
    )
  )

  println("📋 Final schema (for ML):")
  finalDf.printSchema()

  println(s"🔢 Total final records: ${finalDf.count()}")

  println("👀 Final sample rows:")
  finalDf.show(5, truncate = false)

  println(s"💾 Writing ML-ready features to Parquet at: $outputPath")

  finalDf.write
    .mode("overwrite")
    .parquet(outputPath)

  println(s"✅ Wrote preprocessed data to $outputPath")

} catch {
  case e: Exception => println(s"❌ Error: ${e.getMessage}")
}