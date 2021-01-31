package covidtweetstats

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("covid-tweet-stats")
      .getOrCreate()
    val tweetDF = spark.read
      .option("header", "true")
      .option("delimiter", "\t")
      .csv("s3a://adam-king-848/data/q4_a_full.tsv")
      .cache()

    println(s"Trend of discussion is ${TrendPercentChange.sincePeak(tweetDF, spark)} since peak")
    println(s"Compared to last month, trend of discussion is ${TrendPercentChange.latestMonth(tweetDF, spark)}")
    println(s"Compared to last week, trend of discussion is ${TrendPercentChange.latestWeek(tweetDF, spark)}")
    println(s"Compared to yesterday, trend of discussion is ${TrendPercentChange.sincePreviousDay(tweetDF, spark)}")

    TweetCount.dailyCountsChronological(tweetDF, spark)
  }
}