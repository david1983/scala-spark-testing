name := "scala-spark"

version := "0.1"

scalaVersion := "2.11.6"

mainClass := Some("prog.Main")

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := false

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib_2.11
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.2.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.2.0" % "test"
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.8.0" % "test"
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.4" % "test"
libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.13.4" % "test"