package covidcases

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("covid-cases")
      .getOrCreate()
    val covidDF = spark.read
      .option("header", "true")
      .option("delimiter", "\t")
      .csv("s3a://adam-king-848/data/CDC_Covid_archive.tsv")
      .cache()

    CovidCases.casesByCount(covidDF, spark)
  }
}