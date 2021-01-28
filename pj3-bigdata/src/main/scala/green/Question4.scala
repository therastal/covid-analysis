package green

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{avg, count, desc}

object Question4 {
  
  // Need to initialize these somewhere (probably in a file io class)
  //
  // val df = spark.read
  //   .option("header", "true")
  //   .option("delimiter", "\t")
  //   .csv("s3a://adam-king-848/data/q4_a_full.tsv")
  //
  // val df = spark.read
  //   .option("header", "true")
  //   .option("delimiter", "\t")
  //   .csv("s3a://adam-king-848/data/CDC_Covid_archive.tsv")

  
  /**
   * Shows How many times a day there are infections ordered by date
   * @param spark the Spark Session
   */
  def getCasesByDate(df: DataFrame, spark: SparkSession): Dataset[Row] = {
    import spark.implicits._

    // df from CDC_Covid_archive.tsv

    //filters to only get ages 0 - 29, then groups by days and orders by date
    val dataSpikes = df
      .select($"cdc_report_dt", $"age_group")
      .filter($"age_group" === "0 - 9 Years" || $"age_group" === "10 - 19 Years" || $"age_group" === "20 - 29 Years")
      .groupBy("cdc_report_dt")
      .count()
      .withColumnRenamed("cdc_report_dt", "Date")
      .orderBy(desc("Date"))

    dataSpikes
  }

  /**
   * Shows How many times a day there are infections ordered by infection count
   * @param spark the Spark Session
   */
  def getCasesByCount(df: DataFrame, spark: SparkSession): Dataset[Row] = {
    import spark.implicits._

    // df from CDC_Covid_archive.tsv

    //filters to only get ages 0 - 29, then groups by days and orders by amount of infections
    val dataSpikes = df
      .select($"cdc_report_dt", $"age_group")
      .filter($"age_group" === "0 - 9 Years" || $"age_group" === "10 - 19 Years" || $"age_group" === "20 - 29 Years")
      .groupBy("cdc_report_dt")
      .count()
      .withColumnRenamed("cdc_report_dt", "Date")
      .orderBy(desc("Count"))

    dataSpikes
  }
  
  /**
   * Shows How many times a day there are infections ordered by infection count in ages 0-9
   * @param spark the Spark Session
   */
  def getCasesByCount09(df: DataFrame, spark: SparkSession): Dataset[Row] = {
    import spark.implicits._

    // df from CDC_Covid_archive.tsv

    //filters to only get ages 0 - 9, then groups by days and orders by amount of infections
    val dataSpikes = df
      .select($"cdc_report_dt", $"age_group")
      .filter($"age_group" === "0 - 9 Years")
      .groupBy("cdc_report_dt")
      .count()
      .withColumnRenamed("cdc_report_dt", "Date")
      .orderBy(desc("Count"))

    dataSpikes
  }
  
  /**
   * Shows How many times a day there are infections ordered by infection count in ages 10-19
   * @param spark the Spark Session
   */
  def getCasesByCount1019(df: DataFrame, spark: SparkSession): Dataset[Row] = {
    import spark.implicits._

    // df from CDC_Covid_archive.tsv

    //filters to only get ages 10 - 19, then groups by days and orders by amount of infections
    val dataSpikes = df
      .select($"cdc_report_dt", $"age_group")
      .filter($"age_group" === "10 - 19 Years")
      .groupBy("cdc_report_dt")
      .count()
      .withColumnRenamed("cdc_report_dt", "Date")
      .orderBy(desc("Count"))

    dataSpikes
  }
  
  /**
   * Shows How many times a day there are infections ordered by infection count in ages 20-29
   * @param spark the Spark Session
   */
  def getCasesByCount2029(df: DataFrame, spark: SparkSession): Dataset[Row] = {
    import spark.implicits._

    // df from CDC_Covid_archive.tsv

    //filters to only get ages 20 - 29, then groups by days and orders by amount of infections
    val dataSpikes = df
      .select($"cdc_report_dt", $"age_group")
      .filter($"age_group" === "20 - 29 Years")
      .groupBy("cdc_report_dt")
      .count()
      .withColumnRenamed("cdc_report_dt", "Date")
      .orderBy(desc("Count"))

    dataSpikes
  }
  
  /**
   * Shows How many times a day there are infections ordered by infection count in ages 30-39
   * @param spark the Spark Session
   */
  def getCasesByCount3039(df: DataFrame, spark: SparkSession): Dataset[Row] = {
    import spark.implicits._

    // df from CDC_Covid_archive.tsv

    //filters to only get ages 30 - 39, then groups by days and orders by amount of infections
    val dataSpikes = df
      .select($"cdc_report_dt", $"age_group")
      .filter($"age_group" === "30 - 39 Years")
      .groupBy("cdc_report_dt")
      .count()
      .withColumnRenamed("cdc_report_dt", "Date")
      .orderBy(desc("Count"))

    dataSpikes
  }

  def printTrendingStatus(spark: SparkSession): Unit = {
    val path = "s3a://adam-king-848/data/q4_a_full.tsv"
    val df = spark.read
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(path)
      .cache()

    println(s"Trend of discussion is ${TrendPercentChange.sincePeak(df, spark)} since peak")
    println(s"Compared to last month, trend of discussion is ${TrendPercentChange.latestMonth(df, spark)}")
    println(s"Compared to last week, trend of discussion is ${TrendPercentChange.latestWeek(df, spark)}")
    println(s"Compared to yesterday, trend of discussion is ${TrendPercentChange.sincePreviousDay(df, spark)}")
  }

  def printQuestion4(spark : SparkSession): Unit = {
    val df1 = spark.read
      .option("header", "true")
      .option("delimiter", "\t")
      .csv("s3a://adam-king-848/data/q4_a_full.tsv")
      .cache()

    val df2 = spark.read
      .option("header", "true")
      .option("delimiter", "\t")
      .csv("s3a://adam-king-848/data/CDC_Covid_archive.tsv")
      .cache()

    TweetCount.countsByDate(df1, spark)
    getCasesByCount(df2, spark)
  }

}
