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
}
