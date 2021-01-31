package covidtweetstats

import org.scalatest.flatspec.AnyFlatSpec

class TrendPercentChangeTest extends AnyFlatSpec {
  val spark = TestSparkSession.spark
  import spark.implicits._
  val testDF = DummyData.df

  "sincePeak" should "return -50" in {
    assert(TrendPercentChange.sincePeak(testDF, spark) == -50)
  }
}