package covidtweetstats

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{avg, count, desc}

object TweetCount {

  /** Returns the highest number of tweets from a single date. */
  def peak(df: DataFrame, spark: SparkSession): Long = {
    import spark.implicits._

    dailyCountsRanked(df, spark)
      .first()
      .getLong(1)
  }

  /** Returns the number of tweets on the most recent date. */
  def latest(df: DataFrame, spark: SparkSession): Long = {
    import spark.implicits._

    dailyCountsChronological(df, spark)
      .take(2)
      .last(1)
      .toString
      .toLong
  }

  /** Returns the number of tweets on the date previous to the latest date. */
  def previous(df: DataFrame, spark: SparkSession): Long = {
    import spark.implicits._

    dailyCountsChronological(df, spark)
      .take(3)
      .drop(2)(0)
      .getLong(1)
  }

  /** Returns the average number of tweets over the latest week. */
  def weekAvg(df: DataFrame, spark: SparkSession): Int = {
    import spark.implicits._

    val week = dailyCountsChronological(df, spark)
      .take(9)
      .drop(2)

    combineCounts(week) / 7
  }

  /** Returns the average number of tweets over the latest month. */
  def monthAvg(df: DataFrame, spark: SparkSession): Int = {
    import spark.implicits._

    val month = dailyCountsChronological(df, spark)
      .take(32)
      .drop(2)

    combineCounts(month) / 30
  }
  
  /** 
    * Returns a Dataset[Row] containing the tweet count from each date in the
    * DataFrame, in reverse chronological order. 
    */
  def dailyCountsChronological(df: DataFrame, spark: SparkSession): Dataset[Row] = {
    import spark.implicits._

    df.groupBy($"date")
      .agg(count("*") as "num_tweets")
      .orderBy(desc("date"))
  }
  
  /** 
    * Returns a Dataset[Row] containing the tweet count from each date, sorted in 
    * descending order by number of tweets.
    */
  def dailyCountsRanked(df: DataFrame, spark: SparkSession): Dataset[Row] = {
    import spark.implicits._

    dailyCountsChronological(df, spark)
      .orderBy(desc("num_tweets"))
  }

  /**
    * Returns the total combined tweet count of all dates from the time period
    * pass in.
    */
  def combineCounts(counts: Array[Row]): Int = {
    // 
    counts
      .map(_.getLong(1))
      .reduce(_ + _)
      .toInt
  }
}