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

package com.snowplowanalytics.piinguin.server.clients

// Scanamo
import com.gu.scanamo._
import com.gu.scanamo.error.DynamoReadError
import com.gu.scanamo.syntax._
import com.gu.scanamo.ops.ScanamoOps

// DynamoDb
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDBAsync, AmazonDynamoDBAsyncClientBuilder}
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult

// Logging
import org.slf4j.LoggerFactory

// Scala
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

// Generated
import com.snowplowanalytics.piinguin.server.generated.protocols.piinguin._

/**
 * Piinguin Client for dynamo
 * @param client the amazon dynamo client to be used
 * @param tableName the name of the table to be used (it must exist)
 */
class DynamoDBClient(val client: AmazonDynamoDBAsync, val tableName: String)(implicit ec: ExecutionContext) {
  private val logger                  = LoggerFactory.getLogger(classOf[DynamoDBClient].getName)
  private val table: Table[PiiRecord] = Table[PiiRecord](tableName)

  /**
   * Write a single record asynchronously
   * @param record PiiRecord object to be written
   * @return a future for the execution of the request with either an error or the odl record if replaced
   */
  def putRecord(record: PiiRecord): Future[Either[String, String]] = exec(table.put(record)).transform {
    case Success(None)                   => ok("OK")
    case Success(Some(Right(piiRecord))) => ok(s"Replaces $piiRecord")
    case Success(Some(Left(error)))      => err(s"$error")
    case Failure(f)                      => err(f.getMessage)
  }

  /**
   * Get a single record asynchronously
   * @param modifiedValue the value for the table key
   * @return a future for the execution of the request with either an error or the record if found
   */
  def getRecord(modifiedValue: String): Future[Either[String, PiiRecord]] =
    exec(table.get('modifiedValue -> modifiedValue)).transform {
      case Success(None) => {
        logger.debug("Reading record: not found")
        err("Record not found")
      }
      case Success(Some(Right(piiRecord))) => {
        logger.debug(s"Reading record: ok $piiRecord")
        ok(piiRecord)
      }
      case Success(Some(Left(error))) => err(s"$error")
      case Failure(f)                 => err(f.getMessage)
    }

  /**
   * Delete a single record asynchronously
   * @param modifiedValue the value for the table key
   * @return a future for the execution of the request with the result of the deletion
   */
  def deleteRecord(modifiedValue: String): Future[Either[String, String]] =
    exec(table.delete('modifiedValue -> modifiedValue)).transform {
      case Success(_) => ok("OK")
      case Failure(f) => err(s"$f")
    }

  /**
   * Write mutltiple records asynchronously
   * @param records List of PiiRecord objects to be written
   * @return a future with the result of the batch write
   */
  def putRecords(records: List[PiiRecord]): Future[List[BatchWriteItemResult]] = exec(table.putAll(records.toSet))

  /**
   * Get mutltiple records asynchronously
   * @param records the list of requests containing the modifiedValues to be read
   * @return a future for the execution of the request with a set of either an error or the record if found
   */
  def getRecords(records: List[ReadPiiRecordRequest]): Future[Set[Either[DynamoReadError, PiiRecord]]] =
    exec(table.getAll('modifiedValue -> records.map(_.modifiedValue).toSet))

  /**
   * Delete mutltiple records asynchronously
   * @param records the list of requests containing the modifiedValues to be deleted
   * @return a future for the execution of the request with a list of the results
   */
  def deleteRecords(records: List[DeletePiiRecordRequest]): Future[List[BatchWriteItemResult]] =
    exec(table.deleteAll('modifiedValue -> records.map(_.modifiedValue).toSet))

  private def exec[A](ops: ScanamoOps[A]): Future[A] = ScanamoAsync.exec(client)(ops)

  /**
   * Private methods for responses with either a success "ok" or "err" for writing and reading calls
   */
  private def ok[A](message: A): Success[Either[String, A]] =
    Success(Right(message))
  private def err[A](message: String): Success[Either[String, A]] =
    Success(Left(message))

}

/**
 * Normal client for dynamodb
 */
object DynamoDBClient {
  lazy val client = AmazonDynamoDBAsyncClientBuilder.defaultClient()
}
