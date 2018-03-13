/*
 * Copyright (c) 2018 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */

package com.snowplowanalytics.piinguin.server

// Scala
import scala.concurrent.ExecutionContext

// Logging
import org.slf4j.LoggerFactory

// Scopt
import scopt.OptionParser

// This project
import clients.DynamoDBClient

// Generated BuildInfo
import com.snowplowanalytics.piinguin.BuildInfo

/**
 * Entry point
 */
object Main {
  implicit val ec              = ExecutionContext.global
  private val logger           = LoggerFactory.getLogger(classOf[PiinguinServer].getName)
  private val APPLICATION_NAME = "piinguin-server"

  private val configParser = new OptionParser[PiinguinServerConfig](APPLICATION_NAME) {
    head(APPLICATION_NAME, BuildInfo.version, BuildInfo.scalaVersion, BuildInfo.sbtVersion, BuildInfo.builtAtString)
    help("help").text("prints this help message")
    version("version").text("prints the server version")
    opt[String]('h', "host")
      .required()
      .text("host to bind server to")
      .action((x, c) => c.copy(host = x))
    opt[Int]('p', "port")
      .required()
      .text("port number to bind server to")
      .action((x, c) => c.copy(port = x))
    opt[String]('t', "table-name")
      .required()
      .text("the dynamodb table to use")
      .action((x, c) => c.copy(tableName = x))
  }

  def main(args: Array[String]) = {
    val executionStart = System.currentTimeMillis
    val config = configParser.parse(args, PiinguinServerConfig()) match {
      case Some(config) => config
      case None         => sys.exit(1)
    }
    logger.info(
      s"Starting $APPLICATION_NAME version: ${BuildInfo.version} built: ${BuildInfo.builtAtString} using scala: ${BuildInfo.scalaVersion} sbt: ${BuildInfo.sbtVersion}")

    val server = new PiinguinServer(config.host, config.port, config.tableName, DynamoDBClient.client)

    server.start
    logger.info(s"Listening on port: ${config.port}")
    server.blockUntilShutdown
    logger.info(s"Exiting after: ${System.currentTimeMillis - executionStart} millis of execution")
  }
}
