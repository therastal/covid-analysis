lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.12.12"
    )),
    name := "covid-tweet-stats"
  )

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.3" % Test
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"