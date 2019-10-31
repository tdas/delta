/*
 * Copyright 2019 Databricks, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

name := "example"
organization := "com.example"
organizationName := "example"
scalaVersion := "2.12.8"
version := "0.1.0"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "2.6.5"  // Same version as Spark 2.4.x
)

unmanagedJars in Compile += file("/Users/tdas/Projects/Databricks/delta-td/target/scala-2.12/delta-core-assembly-0.4.1-SNAPSHOT.jar")
