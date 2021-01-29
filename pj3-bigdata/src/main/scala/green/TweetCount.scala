package green

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{avg, count, desc}

object TweetCount {

  // the df for all of these is:
  // val df = spark.read
  //   .option("header", "true")
  //   .option("delimiter", "\t")
  //   .csv("s3a://adam-king-848/data/q4_a_full.tsv")

  /** Returns the highest number of tweets from a single date. */
  def peak(df: DataFrame, spark: SparkSession): Long = {
    import spark.implicits._

    countsByDateDesc(df, spark)
      .first()
      .getLong(1)
  }

  /** Returns the number of tweets on the most recent date. */
  def latest(df: DataFrame, spark: SparkSession): Long = {
    import spark.implicits._

    countsByDate(df, spark)
      .take(2)
      .last(1)
      .toString
      .toLong
  }

  /** Returns the number of tweets on the date previous to the latest date.*/
  def previous(df: DataFrame, spark: SparkSession): Long = {
    import spark.implicits._

    countsByDate(df, spark)
      .take(3)
      .drop(2)(0)
      .getLong(1)
  }

  /** Returns the average number of tweets over the latest week. */
  def weekAvg(df: DataFrame, spark: SparkSession): Int = {
    import spark.implicits._

    val week = countsByDate(df, spark)
      .take(9)
      .drop(2)

    combineCounts(week) / 7
  }

  /** Returns the average number of tweets over the latest month. */
  def monthAvg(df: DataFrame, spark: SparkSession): Int = {
    import spark.implicits._

    val month = countsByDate(df, spark)
      .take(32)
      .drop(2)

    combineCounts(month) / 30
  }
  
  /** Returns a Dataset[Row] containing the tweet count from each date in the DataFrame. */
  def countsByDate(df: DataFrame, spark: SparkSession): Dataset[Row] = {
    import spark.implicits._

    df.select($"date")
      .groupBy($"date")
      .agg(count($"date") as "num_tweets")
      .orderBy(desc("date"))
  }
  
  /** 
    * Returns a Dataset[Row] containing the tweet count from each date, sorted in 
    * descending order by number of tweets.
    */
  def countsByDateDesc(df: DataFrame, spark: SparkSession): Dataset[Row] = {
    import spark.implicits._

    countsByDate(df, spark)
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