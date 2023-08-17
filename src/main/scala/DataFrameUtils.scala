import org.apache.spark.sql.DataFrame

object DataFrameUtils {
  def filterDataFrame(df: DataFrame): DataFrame = df
    .drop("srcIP", "dstIP", "srcMac", "dstMac", "timeFirst", "timeLast",
      "srcIPCC", "dstIPCC", "srcIPOrg", "dstIPOrg", "srcMac_dstMac_numP", "dstPortClass",
      "srcMacLbl_dstMacLbl", "ethVlanID", "hdrDesc")
    .na.drop()
}
