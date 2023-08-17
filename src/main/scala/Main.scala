import org.apache.spark.sql.SparkSession

@main def run(): Unit =
  val sc = getSparkSession("Random Forest")
  sc.sparkContext.setLogLevel("ERROR")

  val cleanedNoVpnDf = DataFrameUtils.getCleanedDataFrame(sc, "data/train_no_vpn.csv", "false")
  val cleanedVpnDf = DataFrameUtils.getCleanedDataFrame(sc, "data/train_vpn.csv", "true")

  val (trainingData, testingData) = DataFrameUtils.getSplitData(cleanedNoVpnDf, cleanedVpnDf)
  IoUtils.printDataFrame(trainingData)
  IoUtils.printDataFrame(testingData)

  cleanedNoVpnDf.write.json("temp/trainingData")
  cleanedVpnDf.write.json("temp/testingData")


private def getSparkSession(name: String): SparkSession = SparkSession.builder()
  .appName(name)
  .config("spark.master", "local")
  .getOrCreate()
