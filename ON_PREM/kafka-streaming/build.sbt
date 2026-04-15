ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "KafkaSample"
  )

//libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"
//libraryDependencies += "com.lihaoyi" %% "requests" % "0.8.0"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.4.1" % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.4.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "3.4.1" % "provided",
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
  "com.lihaoyi" %% "requests" % "0.8.0" // Required to support `requests._` import
)


// Custom test directory
Test / scalaSource := baseDirectory.value / "big_data_project" / "kafka-streaming" / "src" / "test"


// Only console test output
testOptions += Tests.Argument("-oDF")

// Register test framework
testFrameworks += new TestFramework("org.scalatest.tools.Framework")


