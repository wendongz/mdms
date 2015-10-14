name := "MDM"

version := "1.0"

//val sparkVersion = "1.4.1"
val sparkVersion = "1.5.0"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.5.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.5.0"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.0"

libraryDependencies += "org.apache.parquet" % "parquet" % "1.8.1"

libraryDependencies += "com.databricks" % "spark-csv_2.11" % "1.2.0"

//libraryDependencies += "org.viz.lightning" %% "lightning-scala" % "0.1.6" 
libraryDependencies += "com.quantifind" %% "wisp" % "0.0.4"

libraryDependencies += "org.scalanlp" %% "breeze-viz" % "0.11.2"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

