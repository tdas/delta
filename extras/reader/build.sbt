name := "delta-base"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  // "org.apache.spark" %% "spark-core" % "2.4.2" % "provided",
  "org.apache.spark" %% "spark-catalyst" % "2.4.2" % "provided" exclude("org.apache.spark", "spark-core_2.11"),
  "org.apache.hadoop" % "hadoop-client" % "2.6.5" % "provided" exclude("com.fasterxml.jackson.core", "*"),
  "org.json4s" %% "json4s-jackson" % "3.5.3" exclude("com.fasterxml.jackson.core", "*"),
  "com.fasterxml.jackson.core" % "jackson-core" % "2.6.7",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.6.7",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.1",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7.1",



  // Test deps
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "junit" % "junit" % "4.12" % "test",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "org.apache.spark" %% "spark-catalyst" % "2.4.2" % "test" classifier "tests",
  "org.apache.spark" %% "spark-core" % "2.4.2" % "test" classifier "tests",
  "org.apache.spark" %% "spark-sql" % "2.4.2" % "test" classifier "tests"
)

