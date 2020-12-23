package solution

import java.util.{Calendar, Date}

import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.sql.functions.{col, hour, lit, udf, unix_timestamp}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}

object Task2 {
  def oneHost(data: DataFrame, oneHostColumn: String, oneHostOut: String): DataFrame = {
    val oneHostModel = new OneHotEncoder()
      .setInputCol(oneHostColumn)
      .setOutputCol(oneHostOut)
    oneHostModel.transform(data)
  }

  private def timeToDayOfWeek(day: Date): Int = {
    try {
      val c = Calendar.getInstance()
      c.setTime(day)
      c.get(Calendar.DAY_OF_WEEK);
    } catch {
      case e: Exception => {
        e.printStackTrace()
        -1
      }
    }
  }

  val udfDayOfWeekInt = udf((day: Date) => timeToDayOfWeek(day))

  private val latJFK: Double = 40.641766
  private val lonJFK: Double = -73.780968


  def latlonToDistanceKm(lat1: Double, lon1: Double): Double = {
    val R = 6371;
    val dlat = deg2rad(lat1 - latJFK)
    val dlon = deg2rad(lon1 - lonJFK)
    val a = Math.sin(dlat / 2) * Math.sin(dlat / 2) +
      Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(latJFK)) *
        Math.sin(dlon / 2) * Math.sin(dlon / 2)

    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    val d = R * c
    d
  }

  def deg2rad(deg: Double): Double = {
    deg * (Math.PI / 180)
  }

  def checkDistanceToJFK(lat1: Double, lon1: Double, distanceSatisfy: Int): Int = {
    val distance = latlonToDistanceKm(lat1, lon1)
    if (distance < distanceSatisfy) {
      1
    } else {
      -1
    }
  }

  val udfCheckJFK = udf((lat1: Double, lon1: Double, distanceSatisfy: Int) => checkDistanceToJFK(lat1, lon1, distanceSatisfy))

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("create parquet file")
      .master("local[4]")
      .getOrCreate()

    var data = spark.read.parquet("green_trip_parquet")
      .withColumn("hour_pick_up", hour(col("lpep_pickup_datetime")))
      .withColumn("hour_dropoff", hour(col("Lpep_dropoff_datetime")))
      .withColumn("day_of_week_pick_up", udfDayOfWeekInt(col("lpep_pickup_datetime")))
      .withColumn("day_of_week_dropoff", udfDayOfWeekInt(col("Lpep_dropoff_datetime")))
      .withColumn("duration_in_second", col("Lpep_dropoff_datetime").cast(LongType) - col("lpep_pickup_datetime").cast(LongType))
      .withColumn("pick_up_at_jfk", udfCheckJFK(col("Pickup_latitude"), col("Pickup_longitude"), lit(10)))
      .withColumn("Dropoff_at_jfk", udfCheckJFK(col("Dropoff_latitude"), col("Dropoff_longitude"), lit(2)))

    data = oneHost(data, "hour_pick_up", "hour_pick_up_one_host")
    data = oneHost(data, "hour_dropoff", "hour_dropoff_one_host")
    data = oneHost(data, "day_of_week_pick_up", "day_of_week_pick_up_one_host")
    data = oneHost(data, "day_of_week_dropoff", "day_of_week_dropoff_one_host")

    data.write.mode("overwrite").parquet("task_2")
  }
}
