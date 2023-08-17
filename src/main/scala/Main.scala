import org.apache.spark.sql.SparkSession

@main def run(): Unit =
  val sc = getSparkSession("Random Forest")
  sc.sparkContext.setLogLevel("ERROR")

  val rawDataFrame = IoUtils.readDataFrameFromCSV("data/train_no_vpn.csv", sc)
  IoUtils.printDataFrame(rawDataFrame)

  val filteredDataFrame = DataFrameUtils.filterDataFrame(rawDataFrame)
  IoUtils.printDataFrame(filteredDataFrame)

private def getSparkSession(name: String): SparkSession = SparkSession.builder()
  .appName(name)
  .config("spark.master", "local")
  .getOrCreate()
