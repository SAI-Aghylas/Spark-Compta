package Pretraincsv
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import Pretraincsv.Csvreader


object pretraitementCsv {
  val PATH_TO_FILE="./src/main/Data-files/lettrage et ecriture comptable.csv"
  def main(args: Array[String]): Unit = {
    implicit val spark=SparkSession.builder()
                          .appName("Reading_csv_file")
                          .master("local[2]")
                          .getOrCreate()
    val csvreader=Csvreader(spark)
    val csvFile= csvreader.readcsv(PATH_TO_FILE)
    //csvreader.showcsv(csvFile)
    csvFile
      .withColumn("DATE_MVT", csvFile("DATE_MVT").cast(StringType))
      .withColumn("MONTANT",csvFile("MONTANT").cast(FloatType))
      .select(csvFile("COMPTE_COMPTABLE").alias("Compte_Comptable"))
      .select(csvFile("PIECE_COMPTABLE").alias("Piece_Comptable"))
      .withColumn("type_compte",csvFile("COMPTE_COMPTABLE").substr(1,3) )
      .printSchema()
  }
}
