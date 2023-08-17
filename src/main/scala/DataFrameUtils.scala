import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameUtils {

  /**
   * This method reads the data from the given path and returns a DataFrame
   * dropping the unnecessary columns and adding the feature vector column
   *
   * @param sc    SparkSession
   * @param path  Path to the csv file
   * @param isVpn 0.0 if the data is not VPN, 1.0 if the data is VPN
   * @return DataFrame
   */
  def getCleanedDataFrame(sc: SparkSession, path: String, isVpn: Double): DataFrame =
    val rawDataFrame = IoUtils.readDataFrameFromCSV(path, sc)
    val filteredDataFrame = filterDataFrame(rawDataFrame)
    val cleanedDataFrame = filteredDataFrame.withColumn("isVPN", lit(isVpn))
    val featuredDataFrame = addFeaturesToDataFrame(cleanedDataFrame)
    IoUtils.printDataFrame(featuredDataFrame)
    featuredDataFrame

  /**
   * This method adds the features to the given DataFrame by collecting
   * all numeric features except the isVPN column and assembling them to a vector column
   *
   * @param df DataFrame to add features
   * @return DataFrame with a vector feature column
   */
  private def addFeaturesToDataFrame(df: DataFrame) =
    // Get all numeric features except the isVPN column, which is the label
    val numericFeatures = df
      .schema
      .filterNot(st => st.dataType.equals(StringType) || st.name.equals("isVPN"))
      .map(_.name)

    val assembler = new VectorAssembler()
      .setInputCols(numericFeatures.toArray)
      .setOutputCol("features")
    assembler.transform(df)

  private def filterDataFrame(df: DataFrame) = df
    .drop("srcIP", "dstIP", "srcMac", "dstMac", "timeFirst", "timeLast",
      "srcIPCC", "dstIPCC", "srcIPOrg", "dstIPOrg", "srcMac_dstMac_numP", "dstPortClass",
      "srcMacLbl_dstMacLbl", "ethVlanID", "hdrDesc")
    .na.drop()

  /**
   * This method combines the given DataFrames and splits them into training and testing data
   *
   * @param df1 DataFrame 1
   * @param df2 DataFrame 2
   * @return a tuple of training and testing DataFrames
   */
  def getSplitData(df1: DataFrame, df2: DataFrame): (DataFrame, DataFrame) =
    /**
     * The ratio is calculated as 70% of the minimum of the two DataFrames
     *
     * @return a tuple of ratios for df1 and df2
     */
    def getRatio: (Double, Double) =
      val trainingDataCount = Math.min(df1.count(), df2.count()) * 0.7
      (trainingDataCount / df1.count(), trainingDataCount / df2.count())

    val (df1Ratio, df2Ratio) = getRatio
    val Array(trainingData1, testingData1) = df1.randomSplit(Array(df1Ratio, 1 - df1Ratio))
    val Array(trainingData2, testingData2) = df2.randomSplit(Array(df2Ratio, 1 - df2Ratio))
    val trainingData = trainingData1.union(trainingData2)
    val testingData = testingData1.union(testingData2)
    (trainingData, testingData)

}
