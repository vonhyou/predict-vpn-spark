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
    println(s"Number of rows: ${df.count() - 1}")
    df.show(5)
}
