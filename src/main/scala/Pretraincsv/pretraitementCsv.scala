package Pretraincsv

import org.apache.spark.sql.SparkSession
import Pretraincsv.Csvreader

object pretraitementCsv {
  val PATH_TO_FILE=""
  def main(args: Array[String]): Unit = {
    implicit val spark=SparkSession.builder()
                          .appName("Reading_csv_file")
                          .master("local[2]")
                          .getOrCreate()
    val csvreader=Csvreader(PATH_TO_FILE)

  }
}
