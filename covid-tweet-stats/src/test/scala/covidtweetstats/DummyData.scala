package covidtweetstats

object DummyData {
  val spark = TestSparkSession.spark
  import spark.implicits._
  val df = Seq(
    new TestSchema("1212360731695427584", "2020-01-01", "13:11:37"),
    new TestSchema("1213249613735460865", "2020-01-01", "00:03:43"),
    new TestSchema("1213330173736738817", "2020-01-02", "05:23:50"),
    new TestSchema("1213330173736738812", "2020-01-03", "05:23:50"),
  ).toDF()
}

case class TestSchema(tweetId: String, date: String, time: String)
