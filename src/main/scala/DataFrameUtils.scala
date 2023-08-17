import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFrameUtils {

  def getCleanedDataFrame(sc: SparkSession, path: String, isVpn: Double): DataFrame =
    val rawDataFrame = IoUtils.readDataFrameFromCSV(path, sc)
    val filteredDataFrame = filterDataFrame(rawDataFrame)
    val cleanedDataFrame = filteredDataFrame.withColumn("isVPN", lit(isVpn))
    val featuredDataFrame = addFeaturesToDataFrame(cleanedDataFrame)
    IoUtils.printDataFrame(featuredDataFrame)
    featuredDataFrame

  private def addFeaturesToDataFrame(df: DataFrame): DataFrame =
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

  def getSplitData(df1: DataFrame, df2: DataFrame): (DataFrame, DataFrame) =
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
