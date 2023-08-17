import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SparkSession

@main def run(): Unit =
  val sc = getSparkSession("Random Forest")
  sc.sparkContext.setLogLevel("ERROR")

  val cleanedNoVpnDf = DataFrameUtils.getCleanedDataFrame(sc, "data/train_no_vpn.csv", 0)
  val cleanedVpnDf = DataFrameUtils.getCleanedDataFrame(sc, "data/train_vpn.csv", 1)

  // combine dataframes and split into training and testing data
  val (trainingData, testingData) = DataFrameUtils.getSplitData(cleanedNoVpnDf, cleanedVpnDf)

  val classifier = getRandomForestClassifier("isVPN")
  val model = classifier.fit(trainingData)

  val predications = model.transform(testingData)
  val evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("isVPN")
    .setPredictionCol("prediction")

  val accuracy = evaluator.evaluate(predications)
  println(accuracy)

private def getSparkSession(name: String): SparkSession = SparkSession.builder()
  .appName(name)
  .config("spark.master", "local")
  .getOrCreate()

private def getRandomForestClassifier(labelCol: String) = new RandomForestClassifier()
  .setLabelCol(labelCol)
  .setFeaturesCol("features")
  .setImpurity("gini")
  .setMaxDepth(3)
  .setNumTrees(20)
  .setFeatureSubsetStrategy("auto")