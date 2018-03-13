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
package implementations

// GRPC
import io.grpc.stub.StreamObserver

// Logging
import org.slf4j.LoggerFactory

// Scala
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

// This project
import clients.DynamoDBClient

// Generated
import com.snowplowanalytics.piinguin.server.generated.protocols.piinguin._

/**
 * Implenetation of the GRPC Pinguin server protocol
 */
class PiinguinImpl(dbClient: DynamoDBClient)(implicit ec: ExecutionContext) extends PiinguinGrpc.Piinguin {
  private val logger = LoggerFactory.getLogger(classOf[PiinguinServer].getName)

  /**
   * GRPC call to create a new piiRecord
   */
  override def createPiiRecord(request: CreatePiiRecordRequest): Future[ChangeRecordResponse] = {
    logger.debug(s"Creating record")
    request.piiRecord match {
      case None => Future.successful(errChange("No record in request"))
      case Some(piiRec) =>
        dbClient
          .putRecord(piiRec)
          .map {
            case Right(s) => okChange(s)
            case Left(s)  => errChange(s)
          }
    }
  }

  /**
   * GRPC call to delete a piiRecord
   */
  override def deletePiiRecord(request: DeletePiiRecordRequest): Future[ChangeRecordResponse] = {
    logger.debug(s"Deleting record $request")
    dbClient
      .deleteRecord(request.modifiedValue)
      .map {
        case Right(s) => okChange(s)
        case Left(s)  => errChange(s)
      }

  }

  /**
   * GRPC call to read a piiRecord
   */
  override def readPiiRecord(request: ReadPiiRecordRequest): Future[ReadPiiRecordResponse] = {
    logger.debug(s"Reading record $request")
    dbClient
      .getRecord(request.modifiedValue)
      .map {
        case Right(pii) => okRead("OK", pii)
        case Left(err)  => errRead(err)
      }
  }

  /**
   * GRPC bidirectional streaming request to create pii records
   */
  override def createPiiRecords(
    responseObserver: StreamObserver[ChangeRecordResponse]): StreamObserver[CreatePiiRecordRequest] =
    new StreamObserver[CreatePiiRecordRequest] {

      override def onError(t: Throwable): Unit = responseObserver.onError(t)

      override def onCompleted(): Unit = responseObserver.onCompleted()

      override def onNext(value: CreatePiiRecordRequest): Unit =
        createPiiRecord(value).onComplete {
          case Success(crr) => responseObserver.onNext(crr)
          case Failure(t)   => responseObserver.onError(t)
        }

    }

  /**
   * GRPC bidirectional streaming request to delete pii records
   */
  override def deletePiiRecords(
    responseObserver: StreamObserver[ChangeRecordResponse]): StreamObserver[DeletePiiRecordRequest] =
    new StreamObserver[DeletePiiRecordRequest] {
      override def onError(t: Throwable): Unit = responseObserver.onError(t)

      override def onCompleted(): Unit = responseObserver.onCompleted()

      override def onNext(value: DeletePiiRecordRequest): Unit =
        deletePiiRecord(value).onComplete {
          case Success(crr) => responseObserver.onNext(crr)
          case Failure(t)   => responseObserver.onError(t)
        }

    }

  /**
   * GRPC bidirectional streaming request to read pii records
   */
  override def readPiiRecords(
    responseObserver: StreamObserver[ReadPiiRecordResponse]): StreamObserver[ReadPiiRecordRequest] =
    new StreamObserver[ReadPiiRecordRequest] {
      override def onError(t: Throwable): Unit = responseObserver.onError(t)

      override def onCompleted(): Unit = responseObserver.onCompleted()

      override def onNext(value: ReadPiiRecordRequest): Unit = readPiiRecord(value).onComplete {
        case Success(rrr) => responseObserver.onNext(rrr)
        case Failure(t)   => responseObserver.onError(t)
      }
    }

  /**
   * Private methods for responses with either a success "ok" or "err" for writing and reading calls
   */
  private def errChange(message: String): ChangeRecordResponse =
    new ChangeRecordResponse(Some(Status(message = message, isSuccess = false)))
  private def okChange(message: String): ChangeRecordResponse =
    new ChangeRecordResponse(Some(Status(message = message, isSuccess = true)))

  private def errRead(message: String): ReadPiiRecordResponse =
    new ReadPiiRecordResponse(None, Some(Status(message = message, isSuccess = false)))
  private def okRead(message: String, piiRecord: PiiRecord): ReadPiiRecordResponse =
    new ReadPiiRecordResponse(Some(piiRecord), Some(Status(message = message, isSuccess = true)))
}
