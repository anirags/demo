name := "SparkScalaCourse"

version := "0.1"

scalaVersion := "2.12.12"

// https://mvnrepository.com/artifact/mysql/mysql-connector-java
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.33"
// https://mvnrepository.com/artifact/com.amazon.deequ/deequ


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "org.apache.spark" %% "spark-mllib" % "3.0.0",
  "org.apache.spark" %% "spark-streaming" % "3.0.0",
  "org.twitter4j" % "twitter4j-core" % "4.0.4",
  "org.twitter4j" % "twitter4j-stream" % "4.0.4",
  "org.apache.commons" % "commons-csv" % "1.10.0",
  "com.databricks" %% "spark-xml" % "0.16.0",
  "com.amazon.deequ" % "deequ" % "2.0.0-spark-3.1"
   // libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.33" // Replace with the appropriate version

)
