// spark_ml_final.scala - FULLY CORRECTED VERSION
//
// HOW TO LAUNCH SPARK SHELL FOR THIS SCRIPT:
// Use the corrected, compatible XGBoost version.
//
// spark-shell --master local[*] --packages ml.dmlc:xgboost4j-spark_2.12:1.5.0,ml.dmlc:xgboost4j_2.12:1.5.0
//
// Then, in the shell, type ':paste' and paste all the code below, then press Ctrl+D.

import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.classification._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.Row
import scala.collection.mutable.ListBuffer
import ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier

// --- IMPORTANT PIPELINE CONNECTION ---
// Your PySpark feature extraction script must save its output in PARQUET format for this to work.
// The path must also match.
val featuresPath = "hdfs://localhost:9000/user/hadoop/image_analysis/spark_features"

println(s"Loading features from $featuresPath")
// Reading from Parquet is much more efficient and preserves data types.
val df = spark.read.parquet(featuresPath)

df.printSchema()
println(s"Total rows: ${df.count()}")

// Fix labels using keyword
val data = df.withColumn("label",
  when(lower(col("keyword")) === "cat", 0.0).otherwise(1.0)
)

println("Label distribution after fix:")
data.groupBy("label").count().orderBy("label").show()

// Define all your feature columns
val featureCols = Array(
  "tag_count", "tag_text_len", "has_animals", "has_people", "has_city",
  "r_mean", "g_mean", "b_mean",
  "r_std", "g_std", "b_std",
  "gray_mean", "gray_std",
  "laplacian_var", "edge_density",
  "hog_0","hog_1","hog_2","hog_3","hog_4","hog_5","hog_6","hog_7","hog_8"
)

val clean = data.na.drop(featureCols :+ "label")
println(s"Rows after drop NA: ${clean.count()}")

val Array(train, test) = clean.randomSplit(Array(0.8, 0.2), seed = 42L)

// The VectorAssembler is the key to creating the 'features' column
val assembler = new VectorAssembler()
  .setInputCols(featureCols)
  .setOutputCol("features")
  .setHandleInvalid("skip") // Use "skip" to handle any remaining invalid rows

// Define evaluators
val aucEval = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("rawPrediction")
val accEval = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy")

val results = ListBuffer[(String, Double, Double)]()

def evaluate(fittedModel: PipelineModel, name: String): Unit = {
  val pred = fittedModel.transform(test)
  // Check if the model supports rawPrediction for AUC
  val auc = if (pred.columns.contains("rawPrediction")) aucEval.evaluate(pred) else 0.0
  val acc = accEval.evaluate(pred)
  results += ((name, auc, acc))
  println(f"$name%-25s AUC = $auc%.4f   Accuracy = $acc%.4f")
}

// --- Run Pipeline-based Models ---
println("\n--- Training Standard Models ---")
val lr = new LogisticRegression().setMaxIter(100).setRegParam(0.01)
evaluate(new Pipeline().setStages(Array(assembler, lr)).fit(train), "Logistic Regression")

val rf = new RandomForestClassifier().setNumTrees(100).setMaxDepth(8)
evaluate(new Pipeline().setStages(Array(assembler, rf)).fit(train), "Random Forest")

val dt = new DecisionTreeClassifier().setMaxDepth(8)
evaluate(new Pipeline().setStages(Array(assembler, dt)).fit(train), "Decision Tree")

val svm = new LinearSVC().setMaxIter(100).setRegParam(0.01)
evaluate(new Pipeline().setStages(Array(assembler, svm)).fit(train), "Linear SVM")

val nb = new NaiveBayes()
evaluate(new Pipeline().setStages(Array(assembler, nb)).fit(train), "Naive Bayes")

val layers = Array[Int](featureCols.length, 32, 16, 2)
val mlp = new MultilayerPerceptronClassifier().setLayers(layers).setBlockSize(128).setSeed(42L).setMaxIter(100)
evaluate(new Pipeline().setStages(Array(assembler, mlp)).fit(train), "Neural Network (MLP)")

// XGBoost with compatible library version
println("\n--- Training XGBoost ---")
try {
  val xgb = new XGBoostClassifier(Map(
    "eta" -> 0.1f,
    "max_depth" -> 6,
    "objective" -> "binary:logistic",
    "num_round" -> 50
  )).setFeaturesCol("features").setLabelCol("label")
  evaluate(new Pipeline().setStages(Array(assembler, xgb)).fit(train), "XGBoost")
} catch {
  case e: Throwable => println(s"XGBoost failed: ${e.getMessage}. Skipping.")
}

// --- K-Nearest Neighbors (The Corrected Logic) ---
println("\n--- Training K-Nearest Neighbors (k=5) ---")

// **FIX:** First, run the assembler on your train and test sets to create the 'features' column.
val assembledTrain = assembler.transform(train).select("features", "label")
val assembledTest = assembler.transform(test) // Keep all columns for the final schema

// Now, collect the assembled training data. This will work.
val trainFeatures = assembledTrain.rdd.map { r =>
  (r.getAs[Vector](0), r.getDouble(1))
}.collect()
val bcTrain = sc.broadcast(trainFeatures)

// Map over the assembled test set
val knnPreds = assembledTest.rdd.map { row =>
  val testVec = row.getAs[Vector]("features")
  val neighbors = bcTrain.value.map { case (vec, lab) =>
    (Vectors.sqdist(testVec, vec), lab)
  }.sortBy(_._1).take(5) // k=5
  val pred = neighbors.groupBy(_._2).mapValues(_.length).maxBy(_._2)._1
  // Append the prediction to the original row
  Row.fromSeq(row.toSeq :+ pred)
}

// Create the new schema by adding the prediction column
val knnSchema = assembledTest.schema.add("prediction", "double")

val knnDF = spark.createDataFrame(knnPreds, knnSchema)

// KNN doesn't have a natural 'rawPrediction', so we only calculate accuracy.
val knnAcc = accEval.evaluate(knnDF)
results += (("K-Nearest Neighbors", 0.0, knnAcc)) // AUC is 0.0 as it's not calculated
println(f"K-Nearest Neighbors      AUC = 0.0000   Accuracy = $knnAcc%.4f")


// --- Final Comparison ---
println("\n--- FINAL MODEL COMPARISON (sorted by Accuracy) ---")
val resultsDF = spark.createDataFrame(results.toSeq)
  .toDF("Model", "AUC", "Accuracy")
  .orderBy(col("Accuracy").desc)

resultsDF.show(false)

println("\nALL DONE! Use this table in your report for Technical Depth.")