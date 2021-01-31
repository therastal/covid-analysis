package green

import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.SparkSession

case class TestSchema(tweetId: String, date: String, time: String)

class TweetCountTest extends AnyFlatSpec {

  val spark = SparkSession
    .builder
    .master("local[4]")
    .appName("tweet-count-test")
    .getOrCreate()
  import spark.implicits._

  val testDF = Seq(
    new TestSchema("1212360731695427584", "2020-01-01", "13:11:37"),
    new TestSchema("1213249613735460865", "2020-01-01", "00:03:43"),
    new TestSchema("1213330173736738817", "2020-01-02", "05:23:50"),
    new TestSchema("1213330173736738812", "2020-01-03", "05:23:50"),
  ).toDF()
  
  "peak" should "return 2" in {
    assert(TweetCount.peak(testDF, spark) == 2)
  }

  "latest" should "return 1" in {
    assert(TweetCount.latest(testDF, spark) == 1)
  }
}

/*
 |-- tweet_id: string (nullable = true)
 |-- date: string (nullable = true)
 |-- time: string (nullable = true)
 */