package CreateDimensions
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import Pretraincsv.Csvreader
import CreateDimensions.DimensionCreation
import Pretraincsv.pretraitementCsv._

object Creation {
  def main(args: Array[String]): Unit = {
    implicit val spark=SparkSession.builder()
      .appName("Creating_dimension_tables")
      .master("local[2]")
      .getOrCreate()
    val csvreader=Csvreader(spark)
    val final_csv=csvreader.readcsv(PATH_TO_FILE+FINAL_FILE)
    val min_date=final_csv
      .select(min("DATE_MVT"),min("DATE_REGLEMENT"),min("DATE_ECHEANCE")).collect()

  }

}
