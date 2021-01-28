package green

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{avg, count, desc}

object TrendingTweets {

  // the df for all of these is:
  // val df = spark.read
  //   .option("header", "true")
  //   .option("delimiter", "\t")
  //   .csv("s3a://adam-king-848/data/q4_a_full.tsv")

  //
  // TRENDING PERCENTAGE CHANGES
  //

  def getTrendingPercentageChangeSincePeak(df: DataFrame, spark: SparkSession): String = {
    val peak = getPeakTweetCount(df, spark)
    val currentTweetCount = getTodayTweetCount(df, spark)

    var trendingPercentage : String = s"up ${(((peak - currentTweetCount).abs / peak) * 100).round}%"
    if (peak > currentTweetCount) {
      trendingPercentage = s"down ${(((peak - currentTweetCount) / peak) * 100).round}%"
    }

    val change = (((peak - currentTweetCount) / peak) * 100).round

    trendingPercentage
  }

  def getTrendingPercentageOverMonth(df: DataFrame, spark: SparkSession): String = {
    val avgTweetCountLastMonth = getMonthTweetCount(df, spark).toDouble
    val currentTweetCount = getTodayTweetCount(df, spark).toDouble

    var trendingPercentage : String = s"up ${(((avgTweetCountLastMonth - currentTweetCount).abs / avgTweetCountLastMonth) * 100).round}%"
    if (avgTweetCountLastMonth > currentTweetCount) {
      trendingPercentage = s"down ${(((avgTweetCountLastMonth - currentTweetCount) / avgTweetCountLastMonth) * 100).round}%"
    }

    trendingPercentage
  }

  def getTrendingPercentageOverWeek(df: DataFrame, spark: SparkSession): String = {
    val avgTweetCountLastWeek = getWeekTweetCount(df, spark).toDouble
    val currentTweetCount = getTodayTweetCount(df, spark).toDouble

    var trendingPercentage : String = s"up ${(((avgTweetCountLastWeek - currentTweetCount).abs / avgTweetCountLastWeek) * 100).round}%"
    if (avgTweetCountLastWeek > currentTweetCount) {
      trendingPercentage = s"down ${(((avgTweetCountLastWeek - currentTweetCount) / avgTweetCountLastWeek) * 100).round}%"
    }



    trendingPercentage
  }

  def getTrendingPercentageSinceYesterday(df: DataFrame, spark: SparkSession): String = {
    val avgTweetCountSinceYesterday = getYesterdayTweetCount(df, spark).toDouble
    val currentTweetCount = getTodayTweetCount(df, spark).toDouble

    var trendingPercentage : String = s"up ${(((avgTweetCountSinceYesterday - currentTweetCount).abs / avgTweetCountSinceYesterday) * 100).round}%"
    if (avgTweetCountSinceYesterday > currentTweetCount) {
      trendingPercentage = s"down ${(((avgTweetCountSinceYesterday - currentTweetCount) / avgTweetCountSinceYesterday) * 100).round}%"
    }

    trendingPercentage
  }

  //
  // UTILITY FUNCTIONS FOR FIGURING OUT TRENDING PERCENTAGE CHANGES
  //

  def getPeakTweetCount(df: DataFrame, spark: SparkSession) : Long = {
    import spark.implicits._

    // df from q4_a_full.tsv

    val peak = df
      .select($"date")
      .groupBy($"date")
      .agg(count($"date") as "num_tweets")
      .orderBy(desc("num_tweets"))

    peak.first().getLong(1)
  }


  def getTodayTweetCount(df: DataFrame, spark: SparkSession) : Long = {
    import spark.implicits._

    // df from q4_a_full.tsv

    val currentTweetsForDay = df
      .select($"date")
      .groupBy($"date")
      .agg(count($"date") as "num_tweets")
      .orderBy(desc("date"))
      .take(2)

    currentTweetsForDay.last(1).toString.toLong
  }

  def getMonthTweetCount(df: DataFrame, spark: SparkSession): Int = {
    import spark.implicits._

    // df from q4_a_full.tsv

    val monthAvg = getTweetCounts(df, spark)
      .take(32)
      .drop(2)

    tweetReduce(monthAvg) / 30
  }

  def getWeekTweetCount(df: DataFrame, spark: SparkSession): Int = {
    import spark.implicits._

    // df from q4_a_full.tsv

    val weekAvg = getTweetCounts(df, spark)
      .take(9)
      .drop(2)

    tweetReduce(weekAvg) / 7
  }

  def getYesterdayTweetCount(df: DataFrame, spark: SparkSession): Int = {
    import spark.implicits._

    val dayAvg = getTweetCounts(df, spark)
      .take(3)
      .drop(2)
    
    tweetReduce(dayAvg)
  }

  def tweetReduce(a: Array[Row]): Int = {
    a.map(_.getLong(1))
      .reduce(_ + _)
      .toInt
  }
  
  def getTweetCounts(df: DataFrame, spark: SparkSession): Dataset[Row] = {
    import spark.implicits._

    df.select($"date")
      .groupBy($"date")
      .agg(count($"date") as "num_tweets")
      .orderBy(desc("date"))
  }
  
  def getTweetCountsDesc(df: DataFrame, spark: SparkSession): Dataset[Row] = {
    import spark.implicits._

    getTweetCounts(df, spark)
      .orderBy(desc("num_tweets"))
  }
}