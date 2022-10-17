name := "simple-job"
version := "0.1"
scalaVersion := "2.12.10"
val sparkVersion = "3.2.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion
)


