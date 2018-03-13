//
//  Copyright (c) 2018 Snowplow Analytics Ltd. All rights reserved.
//
//  This program is licensed to you under the Apache License Version 2.0,
//  and you may not use this file except in compliance with the Apache License
//  Version 2.0. You may obtain a copy of the Apache License Version 2.0 at
//  http://www.apache.org/licenses/LICENSE-2.0.
//
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the Apache License Version 2.0 is distributed on
//  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the Apache License Version 2.0 for the specific
//  language governing permissions and limitations there under.
//

// SBT
import sbt._
import Keys._

// Sbt buildinfo plugin
import sbtbuildinfo.BuildInfoPlugin.autoImport._

// Assembly
import sbtassembly.AssemblyPlugin.autoImport._

// Protobuf generator plugin
import sbtprotoc.ProtocPlugin.autoImport.PB

// Scalafmt plugin
import com.lucidchart.sbt.scalafmt.ScalafmtPlugin._
import com.lucidchart.sbt.scalafmt.ScalafmtCorePlugin.autoImport._

// Local DynamoDB plugin
import com.localytics.sbt.dynamodb.DynamoDBLocalKeys._

object BuildSettings {

  lazy val commonSettings = basicSettings ++ formatting ++ commonDependencies

  lazy val clientSettings = Seq(name := "piinguin-client") ++ commonSettings ++ clientDependencies

  lazy val serverSettings = Seq(name := "piinguin-server") ++ localDynamoDbSettings ++ commonSettings ++ serverDependencies ++ assemblySettings

  lazy val protoGenSettings = Seq(name := "piinguin-protocols") ++ basicSettings ++ grpcSources ++ grpcGenDependencies

  lazy val e2eTestSettings = e2eTestDependencies ++ formatting ++ clientDependencies

  lazy val basicSettings = Seq(
    organization := "com.snowplowanalytics",
    version := "0.1.0-rc1",
    scalaVersion := "2.12.6",
    scalacOptions := compilerOptions,
    scalacOptions in Test := Seq("-Yrangepos"),
    javacOptions := javaCompilerOptions,
    parallelExecution in Global := false)

  lazy val compilerOptions = Seq(
    "-deprecation",
    "-encoding", "UTF-8",
    "-feature",
    "-language:implicitConversions",
    "-unchecked",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-unused",
    "-Ywarn-unused-import",
    "-Xfuture",
    "-Xlint",
    "-Xfatal-warnings")

  lazy val javaCompilerOptions = Seq(
    "-source", "1.8",
    "-target", "1.8")

  lazy val buildInfo = Seq(
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "com.snowplowanalytics.piinguin",
    buildInfoOptions += BuildInfoOption.BuildTime)

  lazy val commonDependencies = Seq(
    libraryDependencies ++= Seq(
      Dependencies.Libraries.slf4jApi,
      Dependencies.Libraries.slf4jSimple,
      // Test
      Dependencies.Libraries.specs2))

  lazy val clientDependencies = Seq(
    libraryDependencies ++= Seq(
      Dependencies.Libraries.fs2,
      Dependencies.Libraries.catsEffect
    ))

  lazy val serverDependencies = Seq(
    libraryDependencies ++= Seq(
      Dependencies.Libraries.scopt,
      Dependencies.Libraries.grpcNetty,
      Dependencies.Libraries.scanamo))

  lazy val grpcGenDependencies = Seq(
    libraryDependencies ++= Seq(
      Dependencies.Libraries.grpcNetty,
      Dependencies.Libraries.scalaPBRuntimeGrpc))

  lazy val e2eTestDependencies = Seq(
    libraryDependencies ++= Seq(
      Dependencies.Libraries.scalatest))

  lazy val assemblySettings = Seq(
    assemblyMergeStrategy in assembly := {
      case PathList(ps @ _*) if ps.last endsWith "io.netty.versions.properties" => MergeStrategy.first
      case x =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(x)
    })

  lazy val localDynamoDbSettings = Seq(
    startDynamoDBLocal in Test := (startDynamoDBLocal in Test).dependsOn(compile in Test).value,
    test in Test := (test in Test).dependsOn(startDynamoDBLocal in Test).value,
    testOnly in Test := (testOnly in Test).dependsOn(startDynamoDBLocal in Test).evaluated,
    testOptions in Test += (dynamoDBLocalTestCleanup in Test).value
  )

  lazy val grpcSources = Seq(
    PB.protocVersion := "-v351",
    (PB.targets in Compile) := Seq(scalapb.gen() -> (sourceManaged in Compile).value))

  lazy val formatting = Seq(
    scalafmtConfig := file(".scalafmt.conf"),
    scalafmtOnCompile := true,
    scalafmtVersion := "1.3.0")
}
