package Pretraincsv
import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import Pretraincsv.Csvreader



object pretraitementCsv {

  val PATH_TO_FILE="./src/main/Data-files/"
  val ORIGINAL_FILE="lettrage et ecriture comptable.csv"
  val FINAL_FILE="Lettrage_Compta.csv"

  def main(args: Array[String]): Unit = {
    implicit val spark=SparkSession.builder()
                          .appName("Reading_csv_file")
                          .master("local[2]")
                          .getOrCreate()
    val csvreader=Csvreader(spark)
    val csvFile= csvreader.readcsv(PATH_TO_FILE+ORIGINAL_FILE)

    def replace_empty(ColIn: Column, ReplaceVal: Any): Column = {
     return( when(ColIn.isNull,lit(ReplaceVal)).otherwise(ColIn) )
    }

    // Transform column Types
    val csvUpdateTypes=csvFile
      .withColumn("DATE_MVT", to_date(csvFile("DATE_MVT").cast(StringType),"yyyymmdd"))
      .withColumn("MONTANT",regexp_replace(csvFile("Montant"),",",".").cast(DoubleType))
      .withColumn("LIBELLE_FACTURE",split(csvFile("LIBELLE_FACTURE")," ")(0))
      .withColumn("DATE_REGLEMENT",to_date(csvFile("DATE_REGLEMENT"),"yyyymmdd"))
      .withColumn("DATE_ECHEANCE",to_date(csvFile("DATE_ECHEANCE"),"yyyymmdd"))
      .withColumn("COMPTE_COMPTABLE",col("COMPTE_COMPTABLE").cast(LongType))

    //Fill NUll or Empty values
    val csvFilled=csvUpdateTypes
      .withColumn("DATE_ECHEANCE",replace_empty(col("DATE_ECHEANCE"),current_date()))
      .withColumn("DATE_REGLEMENT",replace_empty(col("DATE_REGLEMENT"),current_date()))
      .withColumn("PIECE_COMPTABLE",regexp_replace(col("PIECE_COMPTABLE"),"      ","AUCUN"))
      .withColumn("HICTSA",regexp_replace(col("HICTSA")," ","X"))


    //Creating new Columns:
    val csvNewColumns=csvFilled
        .withColumn("TYPE_COMPTE",substring(col("COMPTE_COMPTABLE"),1,3))
        .withColumn("ID_ECRITURE",monotonically_increasing_id())

    csvNewColumns.printSchema()
    csvNewColumns.show(50)
    csvreader.storeCsv(csvNewColumns,PATH_TO_FILE+FINAL_FILE)
    //    val tst=csvFilled
    //    tst.groupBy(col("ID_ECRITURE")).count().distinct().show()



    //  csvFilled.groupBy("HICTSA").agg(length(col("HICTSA"))).distinct().show()
//    val tst =csvFilled.withColumn("PIECE_COMPTABLE_FILLED",replace_empty(col("PIECE_COMPTABLE"),"X"))

//      tst.groupBy("PIECE_COMPTABLE").agg(col("PIECE_COMPTABLE"),length(col("PIECE_COMPTABLE"))).distinct().sort("PIECE_COMPTABLE").show(100)
//    tst.select(count("PIECE_COMPTABLE_FILLED")).distinct()

  }
}
