package Pretraincsv

import org.apache.spark.sql.SparkSession
import Pretraincsv.Csvreader

object pretraitementCsv {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder()
                          .appName("Reading_csv_file")
                          .master("local[2]")
                          .getOrCreate()

  }
}
