import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.spark_project.guava.base.Functions

object test{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("create parquet file")
      .master("local[4]")
      .getOrCreate()

    val data = spark.read.parquet("green_trip_parquet")
      .withColumn("hour_pick_up",hour(col("lpep_pickup_datetime")))
      .withColumn("hour_dropoff",hour(col("Lpep_dropoff_datetime")))
      .withColumn("day_of_week",date_format(col("lpep_pickup_datetime"),"E"))
    val oneHostPickUp = new OneHotEncoder()
      .setInputCol("hour_pick_up")
      .setOutputCol("hour_pick_up_one_host")


    val data1 = oneHostPickUp.transform(data)

    data1.show()

  }
}