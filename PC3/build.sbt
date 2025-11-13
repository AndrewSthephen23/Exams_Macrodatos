name := "PC3"
version := "1.0"

scalaVersion := "2.12.10"

val sparkVersion = "3.0.1"

libraryDependencias ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
    "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)
