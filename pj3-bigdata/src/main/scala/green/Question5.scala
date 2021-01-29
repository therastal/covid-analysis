package green.Q5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{col, count, desc, substring}

object Question5 {

  /**
   * Program takes in a large COVID-19 Tweet IDs set (OPTIONAL: Remove commented codes on
   * parquet partitioning and change df into readDf to improve queries runtime). Performs three different
   * queries to answer the question (5) of when was COVID-19 being discussed the most by months, days, and hours
   * of the day that has the highest discussion count.
   *
   * @param spark The SparkSession for SparkSQL
   */
  def getMostDiscussion(spark: SparkSession)= {

    // Location of full dataset tsv file on S3
    val fullDataLocation = "s3a://adam-king-848/data/q4_a_full.tsv"

    // Reading tsv and turning it into a DataFrame
    val tweetIdDF = spark.read
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(fullDataLocation)

    // Create new view for original partitioned files to use pure SQL
    val viewtweetIdDF = tweetIdDF.createOrReplaceTempView("viewDf")

    // Finding count of COVID-19 related tweets per month
    val mostTweetedMonth = tweetIdDF
      .groupBy(month(col("date")).as("month"))
      .agg(count("*").as("tweets"))
      .orderBy(desc("tweets"))
    
    mostTweetedMonth.show()

    // Save result into ONE csv/excel file so that we can graph
    // (using coalesce to merge all parts when the cores are finished --
    // the parts are created based on data locality in HDFS)
    //month.coalesce(1).write.format("com.databricks.spark.csv").save("months_result.csv")

    // Finding count of COVID-19 related tweets per day
    val mostTweetedDay = tweetIdDF
      .groupBy(col("date").as("day"))
      .agg(count("*").as("tweets"))
      .orderBy(desc("tweets"))
    //day.coalesce(1).write.format("com.databricks.spark.csv").save("days_result.csv")

    mostTweetedDay.show()

    // Finding the day that has highest COVID-19 related tweets
    val mostTweetedDayToString = mostTweetedDay
      .head()(0)
      .toString()

    // Finding count of COVID-19 related tweets per hour of the day that has the highest tweets count

    val mostTweetedHour = tweetIdDF
      .filter(col("date") === mostTweetedDayToString)
      .groupBy(hour(col("time")).as("hour"))
      .agg(count("*").as("tweets"))
      .orderBy(desc("tweets"))

    mostTweetedHour.show()
    
    //hour.coalesce(1).write.format("com.databricks.spark.csv").save("hours_result.csv")

  }
}
