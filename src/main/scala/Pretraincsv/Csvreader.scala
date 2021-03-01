package Pretraincsv

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

class Csvreader(spark: SparkSession) {
  def readcsv(path:String): DataFrame ={
//    val schema = new StructType()
//      schema.add("COMPTE_COMPTABLE",IntegerType,true)
//      .add("DATE_MVT",DateType,true)
//      .add("PIECE_COMPTABLE",StringType,true)
//      .add("MONTANT",FloatType,true)
//      .add("SENS",StringType,true)
//      .add("LIBELLE_FACTURE",StringType,true)
//      .add("DATE_REGLEMENT",DateType,true)
//      .add("DATE_ECHEANCE",DateType,true)
//      .add("HICTSA",StringType,true)
//    spark.read.format("csv")
//      .option("header", "true")
//      .option("delimiter", ";")
//      .schema(schema)
//      .load(path)
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
  def storeCsv(csv_file: DataFrame,path:String) :Unit ={
    csv_file.write
      .option("header","true")
      .mode("overwrite")
      .option("sep",";")
      .csv(path)
  }

}
object Csvreader{
  def apply(spark: SparkSession): Csvreader = new Csvreader(spark)

}