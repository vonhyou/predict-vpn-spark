ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.0"

lazy val root = (project in file("."))
  .settings(
    name := "random-forest"
  )

val sparkVersion = "3.4.1"

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % sparkVersion).cross(CrossVersion
    .for3Use2_13),
  ("org.apache.spark" %% "spark-sql" % sparkVersion).cross(CrossVersion
    .for3Use2_13),
  ("org.apache.spark" %% "spark-mllib" % sparkVersion).cross(CrossVersion
    .for3Use2_13)
)
