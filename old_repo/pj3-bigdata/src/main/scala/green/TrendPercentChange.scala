package green

import org.apache.spark.sql.{DataFrame, SparkSession}

object TrendPercentChange {

  /** Returns the change in trending percentage since peak */
  def sincePeak(df: DataFrame, spark: SparkSession): Long = {
    val peak = TweetCount.peak(df, spark).toDouble
    val latest = TweetCount.latest(df, spark).toDouble

    calculateChange(latest, peak)
  }
  
  /** Returns the change in trending percentage since yesterday */
  def sincePreviousDay(df: DataFrame, spark: SparkSession): Long = {
    val previous = TweetCount.previous(df, spark).toDouble
    val latest = TweetCount.latest(df, spark).toDouble
    
    calculateChange(latest, previous)
  }

  /** Returns the change in trending percentage over the past week */
  def latestWeek(df: DataFrame, spark: SparkSession): Long = {
    val week = TweetCount.weekAvg(df, spark).toDouble
    val latest = TweetCount.latest(df, spark).toDouble

    calculateChange(latest, week)
  }

  /** Returns the change in trending percentage over the past month */
  def latestMonth(df: DataFrame, spark: SparkSession): Long = {
    val month = TweetCount.monthAvg(df, spark).toDouble
    val latest = TweetCount.latest(df, spark).toDouble

    calculateChange(latest, month)
  }

  /**
    * Returns the positive or negative change in percentage between `today` 
    * and `other`. 
    */
  def calculateChange(latest: Double, comparison: Double): Long = {
    (((comparison - latest) / comparison) * 100).round
  }
}