import org.apache.spark.sql.{DataFrame, SparkSession}

object IoUtils {
  def readDataFrameFromCSV(path: String, sc: SparkSession): DataFrame = sc.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("mode", "DROPMALFORMED")
    .csv(path)
    .cache()

  def printDataFrame(df: DataFrame): Unit =
    println(s"Number of schema: ${df.schema.length}")
    df.show(5)
}
