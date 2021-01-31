package green

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{avg, count, desc}

object CovidCases {
  
  // Need to initialize these somewhere (probably in a file io class)
  //
  // val df = spark.read
  //   .option("header", "true")
  //   .option("delimiter", "\t")
  //   .csv("s3a://adam-king-848/data/CDC_Covid_archive.tsv")
  
  /** Returns Dataset containing daily cases for ages 0 - 29, ordered by Date. */
  def casesByDate(df: DataFrame, spark: SparkSession): Dataset[Row] = {
    import spark.implicits._

    countDailyCases(df, spark)
      .orderBy(desc("Date"))
  }

  /** Returns Dataset containing daily cases for ages 0 - 29, ordered by Count. */
  def casesByCount(df: DataFrame, spark: SparkSession): Dataset[Row] = {
    import spark.implicits._

    countDailyCases(df, spark)
      .orderBy(desc("Count"))
  }

  /** Returns DataFrame containing daily cases for ages 0 - 29. */
  def countDailyCases(df: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._

    df.filter(
        $"age_group" === "0 - 9 Years" || 
        $"age_group" === "10 - 19 Years" || 
        $"age_group" === "20 - 29 Years"
      )
      .groupBy($"cdc_report_dt".as("Date"))
      .count()
  }

  /** Returns Dataset containing daily cases for ages 0 - 9. */
  def countDailyCasesChildren(df: DataFrame, spark: SparkSession): Dataset[Row] = {
    import spark.implicits._

    df.filter($"age_group" === "0 - 9 Years")
      .groupBy($"cdc_report_dt".as("Date"))
      .count()
      .orderBy(desc("Count"))
  }
  
  /** Returns Dataset containing daily cases for ages 10 - 19. */
  def countDailyCasesTeens(df: DataFrame, spark: SparkSession): Dataset[Row] = {
    import spark.implicits._

    df.filter($"age_group" === "10 - 19 Years")
      .groupBy($"cdc_report_dt".as("Date"))
      .count()
      .orderBy(desc("Count"))
  }
  
  /** Returns Dataset containing daily cases for ages 20 - 29. */
  def countDailyCasesTwenties(df: DataFrame, spark: SparkSession): Dataset[Row] = {
    import spark.implicits._

    df.filter($"age_group" === "20 - 29 Years")
      .groupBy($"cdc_report_dt".as("Date"))
      .count()
      .orderBy(desc("Count"))
  }
  
  /** Returns Dataset containing daily cases for ages 30 - 39. */
  def countDailyCasesThirties(df: DataFrame, spark: SparkSession): Dataset[Row] = {
    import spark.implicits._
    
    df.filter($"age_group" === "30 - 39 Years")
      .groupBy($"cdc_report_dt".as("Date"))
      .count()
      .orderBy(desc("Count"))
  }
}
