
name := "High-Performance-Spark"

version := "0.1"

scalaVersion := "2.11.8"

//resolvers += "Spark Package Main Repo" at "https://dl.bintray.com/spark-packages/maven"
//addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.5")
//sparkVersion := "2.2.0"
//sparkComponents ++= Seq("core", "sql", "hive")


val sparkVersion = "2.2.0"
libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion)