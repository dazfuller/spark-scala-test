name := "databricks_lib"

version := "0.1"

scalaVersion := "2.11.12"

organization := "com.github.dazfuller"

val sparkVersion = "2.4.3"
val scalaTestVersion = "3.0.8"

parallelExecution in Test := false

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)

libraryDependencies ++= Seq(
  "org.scalactic" %% "scalactic" % scalaTestVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  "com.holdenkarau" %% "spark-testing-base" % "2.4.3_0.12.0" % Test
)