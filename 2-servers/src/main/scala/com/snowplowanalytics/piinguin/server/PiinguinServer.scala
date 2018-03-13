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

// Java
import java.net.{InetSocketAddress}

// Logging
import org.slf4j.LoggerFactory

// Dynamo
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync

// GRPC
import io.grpc.Server
import io.grpc.netty.NettyServerBuilder

// Scala
import scala.concurrent.ExecutionContext

// This project
import clients.DynamoDBClient
import implementations.PiinguinImpl

// Generated
import com.snowplowanalytics.piinguin.server.generated.protocols.piinguin.PiinguinGrpc

/**
 * GRPC Piinguin server
 *
 * @param host the address to listen to
 * @param port the port to listen to
 * @param tableName the dynamoDB table name where to connect
 * @param dynamoDbClient the Dynamo DB client to use
 */
class PiinguinServer(host: String, port: Int, tableName: String, dynamoDbClient: AmazonDynamoDBAsync)(
  implicit executionContext: ExecutionContext) { self =>

  private val logger = LoggerFactory.getLogger(classOf[PiinguinServer].getName)

  private val server: Server = NettyServerBuilder
    .forAddress(new InetSocketAddress(host, port))
    .addService(
      PiinguinGrpc.bindService(new PiinguinImpl(new DynamoDBClient(dynamoDbClient, tableName)), executionContext))
    .build

  def start(): Unit = {
    logger.info(s"Server started on port: $port")
    server.start()
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        logger.info(s"Shutdown hook started")
        self.stop()
        logger.info(s"Shutdown hook complete")
      }
    })
  }

  def stop(): Unit = {
    logger.info("Server shutting down")
    server.shutdown()
    logger.info("Server shut down complete")
  }

  def blockUntilShutdown(): Unit = server.awaitTermination
}
