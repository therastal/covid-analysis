package covidtweetstats

import org.apache.spark.sql.SparkSession

object TestSparkSession {
  val spark = SparkSession
    .builder
    .master("local[4]")
    .appName("tweet-count-test")
    .getOrCreate()
}
