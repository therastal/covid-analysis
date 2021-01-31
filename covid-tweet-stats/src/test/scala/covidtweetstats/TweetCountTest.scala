package covidtweetstats

import org.scalatest.flatspec.AnyFlatSpec

class TweetCountTest extends AnyFlatSpec {
  val spark = TestSparkSession.spark
  import spark.implicits._
  val testDF = DummyData.df
  
  "peak" should "return 2" in {
    assert(TweetCount.peak(testDF, spark) == 2)
  }

  "latest" should "return 1" in {
    assert(TweetCount.latest(testDF, spark) == 1)
  }
}