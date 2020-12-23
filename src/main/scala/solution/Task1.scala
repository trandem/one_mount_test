package solution

import org.apache.spark.sql.SparkSession

object Task1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("create parquet file")
      .master("local[4]")
      .getOrCreate()

    val data = spark.read
      .option("delimiter",",")
      .option("header",true)
      .option("inferSchema",true)
      .csv("green_tripdata_2013-09.csv")
      .withColumnRenamed("Trip_type ","Trip_type")

    data.printSchema()

    data.write.mode("overwrite").parquet("green_trip_parquet")
  }
}
