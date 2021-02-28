package Pretraincsv

import org.apache.spark.sql.{DataFrame, SparkSession}

class Csvreader(spark: SparkSession) {
  def readcsv(path:String): DataFrame ={
    spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .csv(path)
  }
  def showcsv(csv_file: DataFrame): Unit ={
    csv_file.show()
  }
  def storeColumn(csv_file: DataFrame):List[String] ={
    csv_file.columns.toList
  }
}
object Csvreader{
  def apply(spark: SparkSession): Csvreader = new Csvreader(spark)

}