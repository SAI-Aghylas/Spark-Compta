package CreateDimensions

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import Pretraincsv.Csvreader._

class DimensionCreation(sparkSession: SparkSession) {
  def create_calendar(df: DataFrame): DataFrame ={
    val calendar_df= df.select("DATE_MVT","DATE_ECHEANCE").collect()
    return df
  }
}
object DimensionCreation{
  def apply(spark: SparkSession): DimensionCreation = new DimensionCreation(spark)
}
